# mh-user-action-harvester

**NOTE**: this project has been deprecated/superceeded by the new, combined ZOOM + Opencast user analtyics harvester, [harvard-dce-user-analytics](https://github.com/harvard-dce/dce-user-analytics)

A python script for fetching useraction events and related metadata from Opencast Matterhorn. The script uses the [pyhorn](https://github.com/harvard-dce/pyhorn) library for interacting with the Matterhorn API. Harvested useractions are fed to an SQS queue.

## Setup

This script is designed in part with the assumption that it will be installed as part of an `mh-opsworks` analytics node, but independent installation/testing is also possible. Just follow these steps:

1. clone this repo somewhere and `cd` into the source directory.
1. Optionally switch into the desired branch: `git checkout <branch>`
1. Optionally activate a python virtualenv: `virtualenv venv; source venv/bin/activate`
1. install the python dependencies: `pip install -r requiements.txt`
1. install and start an instance of [redis](http://redis.io).
1. Optionally, if you want to generate an index of episode metadata via the `load_episodes` command, you'll need to install and start an instance of [elasticsearch](http://elastic.co).
1. copy `example.env` to `.env` and update the env var settings (see below).
1. Use the `--help` option for commands and usage

        ./ua_harvest.py --help
        ./ua_harvest.py harvest --help
        ./ua_harvest.py load_episodes --help

## Commands

The script uses the [click](http://click.pocoo.org/) library to implement its CLI. There are two sub-commands, `harvest` and `load_episodes`, both of which take the `--help` option to display usage.

### `harvest`

This command fetches batches of useraction events based on a `--start` and `--end` timestamp. If a start/end is not specified the script will look for and use the timestamp of the last useraction fetched (stored in an S3 bucket; see settings below) as the start value and `now()` as the end value. If no timestamp is stored in S3 the default is to fetch the last `--interval` minutes of events (defaults to 2 minutes). Events are fetched in batches of `--batch-size` (default 1000) using the API endpoint's `limit` and `offset` parameters. Events are output to an SQS queue identified with `--queue-name`. If `--queue-name` is `"-"` the json data will be sent to stdout.

There is one additional option, `--disable-start-end-span-check`, that prevents the harvester's start/end timestamps from growing too large. See details below.

To reduce load on the engage server during harvesting, redis is used to cache the episode data between harvests.

#### Example `harvest` command:

Assuming your `.env` file has the necessary settings, this will fetch and process the last 1 minute of useractions in batches of 100 events, and dump the output to `stdout`:

        ./ua_harvest.py harvest --batch-size 100 --interval 1 --output -

### `load_episodes`

This command fetches episode metadata from the Matterhorn search endpoint. By default it looks for episodes that have been created in the past 1 day. This can be controlled with the `--created_from_days_ago` option. The episode records are augmented with additional information about live stream start/stop times and availability via the admin node's workflow API endpoint. The resulting records are sent to an Elasticsearch index, identified by `--es_host` and `--es_index`.

## Settings

A local `.env` file will be read automatically and is the preferred way of passing options to the commands. `example.env` contains the list of all settings. A complete `.env` sufficient for executing the `harvest` command would look something like this:

    # .env
    MATTERHORN_REST_USER=mh_rest_user
    MATTERHORN_REST_PASS=mh_rest_pass
    MATTERHORN_ENGAGE_HOST=matterhorn.example.edu
    S3_HARVEST_TS_BUCKET=my-harvest-timestamp-bucket
    S3_LAST_ACTION_TS_KEY=last-action-timestamp
    SQS_QUEUE_NAME=my-harvest-action-queue

Both the S3 bucket/key and SQS queue will be created if they don't already exist. 

It is possible to have the `harvest` command dump the useraction events to `stdout` rather than sent to an SQS queue by including the option `--output -` on the commandline. In that case no `SQS_QUEUE_NAME` is necessary.

To execute the `load_episode` command you will also need to include hostname of your Matterhorn admin node and the host:port combination for an elasticsearch instance.

    # .env
    ...
    MATTERHORN_ADMIN_HOST=admin.matterhorn.example.edu
    ELASTICSEARCH_HOST=localhost:9200

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

#### EPISODE_CACHE_EXPIRE
Time-to-live value for cached episodes fetched during the useraction harvesting. Defaults to 1800s (15m).

#### MAX_START_END_SPAN
Max number of seconds allowed between the useraction start/end timestamps. The harvester will abort if span in seconds is > than this value.

## useraction harvest too-big timespan protection

The `harvest` command has a built-in protection against the start/end time range growing too large. This can happen if the harvester for some reason or other falls behind in fetching the most recent useraction events. For instance, the script fails for some reason or the engage node becomes unresponsive for some length of time. Because the last action timestamp value is only updated on a successful harvest run, the time span the harvester wants to fetch could grow so large that the harvesting process becomes bogged down. To protect against this the harvester will abort if this time span is longer than `MAX_START_END_SPAN` seconds. Leaving `MAX_START_END_SPAN` unset disables this protection.

If the above situation arises there are two courses of action:

1. Manually run the harvester using the `--disable-start-end-span-check` flag. It is recommended that you also ensure no other harvester processes run concurrently by either disabling any cron jobs or using something like `/bin/run-one`.
2. Reset the start time by deleting the S3 object, `s3://<S3_HARVEST_TS_BUCKET>/<S3_LAST_ACTION_TS_KEY`. You'll then need to manually harvest the useraction events that were missed. If the gap is large you can do several runs manipulating the `--start/--end` range as necessary.
