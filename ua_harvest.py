#!/usr/bin/env python

import sys
import json
import boto3
import click
import arrow
import pyhorn
import logging
import requests
import pyloggly
import requests_cache
from os import getenv
from os.path import dirname, join

import time
from repoze.lru import lru_cache
from botocore.exceptions import ClientError

from dotenv import load_dotenv
load_dotenv(join(dirname(__file__), '.env'))

MATTERHORN_REST_USER = getenv('MATTERHORN_REST_USER')
MATTERHORN_REST_PASS = getenv('MATTERHORN_REST_PASS')
MATTERHORN_HOST = getenv('MATTERHORN_HOST')
DEFAULT_INTERVAL = getenv('DEFAULT_INTERVAL', 2)
LOGGLY_TOKEN = getenv('LOGGLY_TOKEN')
LOGGLY_TAGS = getenv('LOGGLY_TAGS')
S3_LAST_ACTION_TS_BUCKET = getenv('S3_LAST_ACTION_TS_BUCKET', 'mh-user-action-harvester')
S3_LAST_ACTION_TS_KEY = getenv('S3_LAST_ACTION_TS_KEY')
SQS_QUEUE_NAME = getenv('SQS_QUEUE_NAME')

# sigh. pyhorn caching is so dumb.
requests_cache.uninstall_cache()
pyhorn.client._session = requests.Session()

log = logging.getLogger('mh-user-action-harvester')
log.setLevel(logging.INFO)
console = logging.StreamHandler(stream=sys.stdout)
console.setFormatter(logging.Formatter("%(name)s %(levelname)s %(message)s"))
log.addHandler(console)
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

@click.command()
@click.option('-s', '--start', help='YYYYMMDDHHmmss')
@click.option('-e', '--end', help='YYYYMMDDHHmmss; default=now')
@click.option('-w', '--wait', default=1,
              help="Seconds to wait between batch requests")
@click.option('-H', '--hostname', default=MATTERHORN_HOST,
              help="Matterhorn engage hostname")
@click.option('-u', '--user', default=MATTERHORN_REST_USER,
              help='Matterhorn rest user')
@click.option('-p', '--password', default=MATTERHORN_REST_PASS,
              help='Matterhorn rest password')
@click.option('-q', '--queue-name', default=SQS_QUEUE_NAME,
              help='SQS queue name')
@click.option('-b', '--batch-size', default=1000,
              help='number of actions per request')
@click.option('-i', '--interval', default=DEFAULT_INTERVAL,
              help='Harvest action from this many mintues ago')
def harvest(start, end, wait, hostname, user, password, queue_name,
            batch_size, interval):

    mh = pyhorn.MHClient('http://' + hostname, user, password, timeout=30)
    queue = get_or_create_queue(queue_name)

    if end is None:
        end = arrow.now().format('YYYYMMDDHHmmss')

    if start is None:
        start = get_last_action_ts()
        if start is None:
            start = arrow.now() \
                .replace(minutes=-interval) \
                .format('YYYYMMDDHHmmss')

    log.info("Fetching user actions from %s to %s", start, end)

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

        actions = mh.user_actions(**req_params)

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
                queue.send_message(MessageBody=json.dumps(rec))
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

    if action_count == 0:
        last_action_ts = end
    else:
        last_action_ts = arrow.get(last_action.created).format('YYYYMMDDHHmmss')
    set_last_action_ts(last_action_ts)

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

    # get the episode directly for easier caching
    episode = get_episode(action.client, action.mediapackageId)

    if episode is not None:
        rec['episode'] = {}
        rec['is_live'] = is_live(action, episode) and 1 or 0

        try:
            rec['episode']['type'] = episode.dcType
        except AttributeError:
            log.warn("Missing type for action %s, episode %s", action.id, episode.id)

        try:
            series = str(episode.mediapackage.series)
            rec['episode'].update({
                'course': episode.mediapackage.seriestitle,
                'title': episode.mediapackage.title,
                'series': series,
                'year': series[:4],
                'term': series[4:6],
                'cdn': series[6:11]
            })
        except AttributeError:
            log.warn("Missing series for action %s, episode %s", action.id, episode.id)
    else:
        log.warn("Missing episode for action %s", action.id)

    return rec

@lru_cache(1000)
def get_episode(client, mpid):
    """
    get the episode direct from client rather than from action.episode so
    that we can better control the caching (via lru_cache memoization)
    """
    try:
        return client.search_episode(mpid)
    except AttributeError:
        return None

def is_live(action, episode):
    """
    here again, if we call action.is_live() we won't get the cached episode resp
    """
    action_created = arrow.get(action.created)
    mp_start = arrow.get(episode.mediapackage.start)
    mp_duration_sec = int(episode.mediapackage.duration) / 1000
    return action_created <= mp_start.replace(seconds=+mp_duration_sec)

# s3 state bucket helpers
def set_last_action_ts(last_action_ts):
    bucket = get_or_create_bucket()
    bucket.put_object(Key=S3_LAST_ACTION_TS_KEY, Body=last_action_ts)

def get_last_action_ts():
    bucket = get_or_create_bucket()
    try:
        obj = bucket.Object(S3_LAST_ACTION_TS_KEY).get()
        return obj['Body'].read()
    except ClientError:
        log.debug("No last_update value found")
        return None

def get_or_create_bucket():
    try:
        s3.meta.client.head_bucket(Bucket=S3_LAST_ACTION_TS_BUCKET)
        return s3.Bucket(S3_LAST_ACTION_TS_BUCKET)
    except ClientError:
        return s3.create_bucket(Bucket=S3_LAST_ACTION_TS_BUCKET)

def get_or_create_queue(queue_name):
    try:
        return sqs.get_queue_by_name(QueueName=queue_name)
    except ClientError:
        return sqs.create_queue(QueueName=queue_name)


if __name__ == '__main__':
    harvest()

