# mh-user-action-harvester

A python script for fetching useraction events and related metadata from Opencast Matterhorn. The script uses the [pyhorn](https://github.com/harvard-dce/pyhorn) library for interacting with the Matterhorn API. Harvested useractions are fed to an SQS queue.

## Setup

1. clone this repo and run `pip install -r requiements.txt`
1. copy `example.env` to `.env` and update the env var settings
3. run `./ua_harvest.py --help` for commands and usage

## Commands

The script uses the [click](http://click.pocoo.org/) library to implement its CLI. There are two sub-commands, `harvest` and `load_episodes`, both of which take the `--help` option to display usage.

### `harvest`

This command fetches batches of useraction events based on a `--start` and `--end` timestamp. If a start/end is not specified the script will look for and use the timestamp of the last useraction fetched (stored in an S3 bucket; see settings below) as the start value and `now()` as the end value. If no timestamp is stored in S3 the default is to fetch the last `--interval` minutes of events (defaults to 2 minutes). Events are fetched in batches of `--batch-size` (default 1000) using the API endpoint's `limit` and `offset` parameters. Events are output to an SQS queue identified with `--queue-name`. If `--queue-name` is `"-"` the json data will be sent to stdout.

There is one additional option, `--disable-start-end-span-check`, that prevents the harvester's start/end timestamps from growing too large. See details below.

### `load_episodes`

This command fetches episode metadata from the Matterhorn search endpoint. By default it looks for episodes that have been created in the past 1 day. This can be controlled with the `--created_from_days_ago` option. The episode records are augmented with additional information about live stream start/stop times and availability via the admin node's workflow API endpoint. The resulting records are sent to an Elasticsearch index, identified by `--es_host` and `--es_index`.

## Settings

A local `.env` file will be read automatically and is the preferred way of passing options to the commands. 

#### MATTERHORN_REST_USER / MATTERHORN_REST_PASS
The user/pass combo for accessing the API.

#### MATTERHORN_ENGAGE_HOST
IP or hostname of the engage node.

#### MATTERHORN_ADMIN_HOST
IP or hostname of the admin node.

#### DEFAULT_INTERVAL
If no `--start` is provided and no last action timestamp is found in s3 the useraction harvest start will be calculated as this many minutes ago.

#### LOGGLY_TOKEN
If present the script will send log events to loggly.

#### LOGGLY_TAGS
Any extra tags to attach to events sent to loggly.

#### S3_HARVEST_TS_BUCKET
Name of the S3 bucket to store the last action's timestamp value.

#### S3_LAST_ACTION_TS_KEY
Name of the S3 object the last action timestamp is saved as.

#### SQS_QUEUE_NAME
Name of the SQS queue to send useraction events. The queue will be created if it doesn't exist.

#### ELASTICSEARCH_HOST
Hostname or IP of the Elasticsearch instance in which to index the episode records.

## useraction harvest too-big timespan protection

The `harvest` command has a built-in protection against the start/end time range growing too large. This can happen if the harvester for some reason or other falls behind in fetching the most recent useraction events. For instance, the script fails for some reason or the engage node becomes unresponsive for some length of time. Because the last action timestamp value is only updated on a successful harvest run, the time span the harvester wants to fetch could grow so large that the harvesting process becomes bogged down. To protect against this the harvester will abort if this time span is longer than 3600 seconds (1 hour). If this situation arises there are two courses of action:

1. Manually run the harvester using the `--disable-start-end-span-check` flag. It is recommended that you also ensure no other harvester processes run concurrently by either disabling any cron jobs or using something like `/bin/run-one`.
2. Reset the start time by deleting the S3 object, `s3://<S3_HARVEST_TS_BUCKET>/<S3_LAST_ACTION_TS_KEY`. You'll then need to manually harvest the useraction events that were missed. If the gap is large you can do several runs manipulating the `--start/--end` range as necessary.
