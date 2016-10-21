#!/usr/bin/env python

import sys
import json
import boto3
import click
import arrow
import redis
import logging
import pyloggly
from os import getenv
from os.path import dirname, join
from pyhorn.endpoints.search import SearchEpisode

import re
from elasticsearch import Elasticsearch

import pyhorn
# force all calls to the episode search endpoint to use includeDeleted=true
pyhorn.endpoints.search.SearchEndpoint._kwarg_map['episode']['includeDeleted'] = True

import time
from botocore.exceptions import ClientError

from dotenv import load_dotenv
load_dotenv(join(dirname(__file__), '.env'))

MAX_START_END_SPAN_SECONDS = 3600
EPISODE_CACHE_EXPIRE = getenv('EPISODE_CACHE_EXPIRE', 1800) # default to 15m

log = logging.getLogger('mh-user-action-harvester')
log.setLevel(logging.DEBUG)
console = logging.StreamHandler(stream=sys.stdout)
console.setFormatter(logging.Formatter("%(name)s %(levelname)s %(message)s"))
log.addHandler(console)

LOGGLY_TOKEN = getenv('LOGGLY_TOKEN')
LOGGLY_TAGS = getenv('LOGGLY_TAGS')
if LOGGLY_TOKEN is not None:
    log.addHandler(
        pyloggly.LogglyHandler(
            LOGGLY_TOKEN,
            'logs-01.loggly.com',
            tags=','.join(['mh-user-action-harvester', LOGGLY_TAGS])
        )
    )

sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')
r = redis.StrictRedis()

@click.group()
def cli():
    pass

@cli.command()
@click.option('-s', '--start', help='YYYYMMDDHHmmss')
@click.option('-e', '--end', help='YYYYMMDDHHmmss; default=now')
@click.option('-w', '--wait', default=1,
              help="Seconds to wait between batch requests")
@click.option('-H', '--engage_host', envvar='MATTERHORN_ENGAGE_HOST',
              help="Matterhorn engage hostname", required=True)
@click.option('-u', '--user', envvar='MATTERHORN_REST_USER',
              help='Matterhorn rest user', required=True)
@click.option('-p', '--password', envvar='MATTERHORN_REST_PASS',
              help='Matterhorn rest password', required=True)
@click.option('-o', '--output', default='sqs',
              help='where to send output. use "-" for json/stdout')
@click.option('-q', '--queue-name', envvar='SQS_QUEUE_NAME',
              help='SQS queue name', required=True)
@click.option('-b', '--batch-size', default=1000,
              help='number of actions per request')
@click.option('-i', '--interval', envvar='DEFAULT_INTERVAL', default=2,
              help='Harvest action from this many mintues ago')
@click.option('--disable-start-end-span-check', is_flag=True,
              help="Don't abort on too-long start-end time spans")
def harvest(start, end, wait, engage_host, user, password, output, queue_name,
            batch_size, interval, disable_start_end_span_check):

    # we rely on our own redis cache, so disable pyhorn's internal response caching
    mh = pyhorn.MHClient('http://' + engage_host, user, password,
                         timeout=30, cache_enabled=False)

    if output == 'sqs':
        queue = get_or_create_queue(queue_name)

    if end is None:
        end = arrow.now().format('YYYYMMDDHHmmss')

    last_action_ts_key = getenv('S3_LAST_ACTION_TS_KEY')
    if start is None:
        start = get_harvest_ts(last_action_ts_key)
        if start is None:
            start = arrow.now() \
                .replace(minutes=-interval) \
                .format('YYYYMMDDHHmmss')

    log.info("Fetching user actions from %s to %s", start, end)

    start_end_span = arrow.get(end, 'YYYYMMDDHHmmss') - arrow.get(start, 'YYYYMMDDHHmmss')
    log.info("Start-End time span in seconds: %d", start_end_span.seconds,
             extra={'start_end_span_seconds': start_end_span.seconds})

    if not disable_start_end_span_check:
        if start_end_span.seconds > MAX_START_END_SPAN_SECONDS:
            log.error("Start-End time span %d is larger than %d",
                      start_end_span.seconds,
                      MAX_START_END_SPAN_SECONDS
                      )
            raise click.Abort()

    offset = 0
    batch_count = 0
    action_count = 0
    fail_count = 0
    last_action = None

    while True:

        req_params = {
            'start': start,
            'end': end,
            'limit': batch_size,
            'offset': offset
        }

        try:
            actions = mh.user_actions(**req_params)
        except Exception, e:
            log.error("API request failed: %s", str(e))
            raise

        if len(actions) == 0:
            log.info("No more actions")
            break

        batch_count += 1
        action_count += len(actions)
        log.info("Batch %d: %d actions", batch_count, len(actions))

        for action in actions:
            last_action = action
            try:
                rec = create_action_rec(action)
                if output == 'sqs':
                    queue.send_message(MessageBody=json.dumps(rec))
                else:
                    print json.dumps(rec)
            except Exception as e:
                log.error("Exception during rec creation for %s: %s", action.id, str(e))
                fail_count += 1
                continue

        time.sleep(wait)
        offset += batch_size

    log.info("Total actions: %d, total batches: %d, total failed: %d",
             action_count, batch_count, fail_count,
             extra={
                 'actions': action_count,
                 'batches': batch_count,
                 'failures': fail_count
             })

    try:
        if action_count == 0:
            last_action_ts = end
        else:
            last_action_ts = arrow.get(last_action.created).format('YYYYMMDDHHmmss')
        set_harvest_ts(last_action_ts_key, last_action_ts)
        log.info("Setting last action timestamp to %s", last_action_ts)
    except Exception, e:
        log.error("Failed setting last action timestamp: %s", str(e))

def create_action_rec(action):

    rec = {
        'action_id': action.id,
        'timestamp': str(arrow.get(action.created).to('utc')),
        'mpid': action.mediapackageId,
        'session_id': action.sessionId['sessionId'],
        'huid': str(action.sessionId.get('userId')),
        'useragent': action.sessionId.get('userAgent'),
        'action': {
            'type': action.type,
            'inpoint': action.inpoint,
            'outpoint': action.outpoint,
            'length': action.length,
            'is_playing': action.isPlaying
        }
    }

    ips = [x.strip() for x in action.sessionId['userIp'].split(',')]
    rec['ip'] = ips.pop(0)
    for idx, ip in enumerate(ips, 1):
        rec['proxy%d' % idx] = ip

    episode = get_episode(action)

    rec['is_live'] = int(action.is_live())
    rec['episode'] = {}

    if episode is None:
        log.warn("Missing episode for action %s", action.id)
    else:
        rec['episode'] = {
            'title': episode.mediapackage.title,
            'duration': int(episode.mediapackage.duration),
            'start': episode.mediapackage.start
        }

        try:
            series = str(episode.mediapackage.series)
            rec['episode'].update({
                'course': episode.mediapackage.seriestitle,
                'series': series,
                'year': series[:4],
                'term': series[4:6],
                'cdn': series[6:11]
            })
        except AttributeError:
            log.warn("Missing series for episode %s", episode.id)

        try:
            rec['episode']['type'] = episode.dcType
        except AttributeError:
            pass

        try:
            rec['episode']['description'] = episode.dcDescription
        except AttributeError:
            pass

    return rec

def get_episode(action):
    cached_ep = r.get(action.mediapackageId)
    if cached_ep is not None:
        log.debug("episode cache hit for %s", action.mediapackageId)
        episode_data = json.loads(cached_ep)
        episode_data['__from_cache'] = True
        # recreate the SearchEpisode obj using the current client
        episode = SearchEpisode(episode_data, action.client)
        # make sure anything else that might access the action.episode property
        # gets our cached version
        action._property_stash['episode'] = episode
    else:
        log.debug("episode cache miss for %s", action.mediapackageId)
        episode = action.episode
        r.setex(
            action.mediapackageId,
            EPISODE_CACHE_EXPIRE,
            json.dumps(episode._raw)
        )
    return episode


@cli.command()
@click.option('-c', '--created_from_days_ago', type=int, default=1,
              help='set createdFrom param to this many days ago')
@click.option('-A', '--admin_host', envvar='MATTERHORN_ADMIN_HOST',
              help="Matterhorn admin hostname", required=True)
@click.option('-E', '--engage_host', envvar='MATTERHORN_ENGAGE_HOST',
              help="Matterhorn engage hostname", required=True)
@click.option('-u', '--user', envvar='MATTERHORN_REST_USER',
              help='Matterhorn rest user', required=True)
@click.option('-p', '--password', envvar='MATTERHORN_REST_PASS',
              help='Matterhorn rest password', required=True)
@click.option('-e', '--es_host', envvar='ELASTICSEARCH_HOST',
              help="Elasticsearch host:port", default='localhost:9200')
@click.option('-i', '--es_index', envvar='EPISODE_INDEX_NAME', default='episodes',
              help="Name of the index for storing episode records")
@click.option('-w', '--wait', default=1,
              help="Seconds to wait between batch requests")
def load_episodes(created_from_days_ago, admin_host, engage_host, user, password, es_host,
                  es_index, wait):

    mh_admin = pyhorn.MHClient('http://' + admin_host, user, password, timeout=30)
    mh_engage = pyhorn.MHClient('http://' + engage_host, user, password, timeout=30)
    es = Elasticsearch([es_host])

    offset = 0
    episode_count = 0
    batch_size = 100

    while True:
        request_params = {
            'offset': offset,
            'limit': batch_size,
            'includeDeleted': True,
            'sort': 'DATE_CREATED'
        }

        if created_from_days_ago is not None:
            request_params['createdFrom'] = arrow.now() \
                .replace(days=-created_from_days_ago).format('YYYY-MM-DDTHH:mm:ss') + 'Z'

        episodes = mh_engage.search_episodes(**request_params)
        if len(episodes) == 0:
            log.info("no more episodes")
            break

        episode_count += len(episodes)
        log.info("fetched %d total episodes", episode_count)

        for ep in episodes:
            doc = {
                'title': ep.mediapackage.title,
                'mpid': ep.mediapackage.id,
                'duration': int(ep.mediapackage.duration),
                'start': str(arrow.get(ep.mediapackage.start).to('utc'))
            }

            try:
                series = str(ep.mediapackage.series)
                doc.update({
                    'course': ep.mediapackage.seriestitle,
                    'series': series,
                    'year': series[:4],
                    'term': series[4:6],
                    'crn': series[6:11]
                })
            except AttributeError:
                series = None
                log.warn("Missing series for episode %s", ep.id)

            try:
                doc['type'] = ep.dcType
            except AttributeError:
                log.warn("Missing type for episode %s", ep.id)

            try:
                doc['description'] = ep.dcDescription
            except AttributeError:
                log.warn("Missing description for episode %s", ep.id)

            attachments = ep.mediapackage.attachments
            if isinstance(attachments, dict):
                try:
                    attachments = attachments['attachment']

                    for preview_type in ['presenter', 'presentation']:
                        try:
                            preview = next(a for a in attachments if a['type'] == '%s/player+preview' % preview_type)
                            doc['%s_still' % preview_type] = preview['url']
                        except StopIteration:
                            pass

                    doc['slides'] = [{
                        'img': a['url'],
                        'time': re.search('time=([^;]+)', a['ref']).group(1)
                    } for a in attachments if a['type'] == 'presentation/segment+preview']

                except Exception, e:
                    log.error("Failed to extract attachement info from episode %s: %s", ep.id, str(e))

            try:
                wfs = mh_admin.workflows(
                    mp=ep.mediapackage.id,
                    state='SUCCEEDED',
                    workflowdefinition='DCE-archive-publish-external'
                    )

                if len(wfs) > 1:
                    raise RuntimeError("Got > 1 workflow for mpid %s" % ep.mediapackage.id)
                else:
                    wf = wfs[0]

                ops = wf.operations

                try:
                    capture = next(x for x in ops if x.id == 'capture')
                    doc.update({
                        'live_stream': 1,
                        'live_start': str(arrow.get(capture.started / 1000)),
                        'live_end': str(arrow.get(capture.completed / 1000)),
                        'live_duration': capture.completed - capture.started
                    })
                except StopIteration:
                    doc['live_stream'] = 0

                try:
                    retract = next(x for x in ops if x.id == 'retract-element')
                    doc['available'] = str(arrow.get(retract.completed / 1000))
                except StopIteration:
                    pass

            except IndexError:
                log.info("No matching or finished workflow found for %s: %s", ep.id, ep.mediapackage.title)
            except Exception, e:
                log.error("Failed extracting workflow data for episode %s: %s", ep.id, str(e))

            es.index(index=es_index,
                     doc_type='episode',
                     id=ep.id,
                     body=doc
                     )

        time.sleep(wait)
        offset += batch_size

# s3 state bucket helpers
def set_harvest_ts(ts_key, timestamp):
    bucket = get_or_create_bucket()
    bucket.put_object(Key=ts_key, Body=timestamp)

def get_harvest_ts(ts_key):
    bucket = get_or_create_bucket()
    try:
        obj = bucket.Object(ts_key).get()
        return obj['Body'].read()
    except ClientError:
        log.debug("No %s value found", ts_key)
        return None

def get_or_create_bucket():

    bucket_name = getenv('S3_HARVEST_TS_BUCKET')
    if bucket_name is None:
        raise RuntimeError("No timestamp bucket specified!")

    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        return s3.Bucket(bucket_name)
    except ClientError:
        return s3.create_bucket(Bucket=bucket_name)

def get_or_create_queue(queue_name):
    try:
        return sqs.get_queue_by_name(QueueName=queue_name)
    except ClientError:
        return sqs.create_queue(QueueName=queue_name)


if __name__ == '__main__':
    cli()
