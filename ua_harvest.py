
import boto3
import click
import arrow
import pyhorn
import requests
from os import getenv
from os.path import dirname, join
from repoze.lru import lru_cache
import requests_cache

# sigh. pyhorn caching is so dumb.
requests_cache.uninstall_cache()

pyhorn.client._session = requests.Session()
from dotenv import load_dotenv
load_dotenv(join(dirname(__file__), '.env'))

MATTERHORN_REST_USER=getenv('MATTERHORN_REST_USER')
MATTERHORN_REST_PASS=getenv('MATTERHORN_REST_PASS')

@click.command()
@click.option('-H', '--hostname')
@click.option('-s', '--start', help='YYYYMMDD')
@click.option('-e', '--end', default=None, help='YYYYMMDD. Default = today')
@click.option('-u', '--user', help='Matterhorn rest user')
@click.option('-p', '--password', help='Matterhorn rest password')
@click.option('-b', '--batch-size', default=1000, help='Actions to process per request')
@click.option('-w', '--wait', default=1, help='seconds to wait between requests')
@click.option('-q', '--queue-name', help='SQS queue name')
def harvest(hostname, start, end, user, password, batch_size, wait, queue_name):

    mh = pyhorn.MHClient('http://' + hostname, user, password, timeout=30)

    start = arrow.get(start_day, 'YYYYMMDD')
    end = (end_day is None) and arrow.now() or arrow.get(end_day, 'YYYYMMDD')

    for day in arrow.Arrow.range('day', start, end):

        print "Working on day %s" % day.format('YYYY-MM-DD')

        req_params = {
            'day': day.format('YYYYMMDD'),
            'limit': batch_size,
            'offset': 0
        }

        out_path = os.path.join(outdir, "actions.%s.jsonl.gz" % day.format('YYYY-MM-DD'))

        with gzip.open(out_path, 'wb') as of:

            batch_count = 1
            while True:

                actions = mh.user_actions(**req_params)

                if len(actions) == 0:
                    break

                print "Batch %d: %d actions" % (batch_count, len(actions))

                for action in actions:
                    try:
                        rec = create_action_rec(action)
                    except Exception as e:
                        print "Exception during rec creation for %s: %s" % (action.id, str(e))
                        continue
                    print >>of, json.dumps(rec)

                time.sleep(wait)
                req_params['offset'] += batch_size
                batch_count += 1

def create_action_rec(action):

    rec = {
        'action_id': action.id,
        'created': action.created,
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

    if episode is None:
        rec['episode'] = {}
        rec['is_live'] = 0
    else:
        try:
            rec['is_live'] = is_live(action, episode) and 1 or 0
            rec['episode'] = {
                'course': episode.mediapackage.seriestitle,
                'title': episode.mediapackage.title,
                'series': episode.mediapackage.series,
                'type': episode.dcType
            }
        except Exception as e:
            pass

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
    here again, if we call action.is_live()
    """
    action_created = arrow.get(action.created)
    mp_start = arrow.get(episode.mediapackage.start)
    mp_duration_sec = int(episode.mediapackage.duration) / 1000
    return action_created <= mp_start.replace(seconds=+mp_duration_sec)

if __name__ == '__main__':
    harvest()

