# Scheduled triggering of functions
import atexit

import logging.config
import logging
logging.config.fileConfig('logging.config')
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")


# from pythonjsonlogger import jsonlogger

# Prometheus utilities
import random
import os
import prometheus_client

# Local class for calling our BigQuery databases
from api.bigquery import BigQuery
from apscheduler.schedulers.background import BackgroundScheduler
# from apscheduler.schedulers.background import BlockingScheduler

# Flask (webapp library) and flask-related dispatcher
from flask import Flask, Response
# from flask_wtf.csrf import CSRFProtect
from werkzeug.middleware.dispatcher import DispatcherMiddleware

import datetime

# Scheduler: keyword arguments (how often to trigger)
kwargs = {"minutes": 5, "next_run_time": datetime.datetime.now()}

# Environment variables
GCP_project = os.environ.get("GCP_PROJECT", "dev-freg-3896")
BQ = BigQuery(GCP_project=GCP_project)
METRIC_PREFIX = "freg_"

# Flask setup
logger.info('Initialising Flask app.')
app = Flask(__name__)
# csrf = CSRFProtect(app)
logger.debug('Setting up prometheus client.')
app.wsgi_app = DispatcherMiddleware(
    app.wsgi_app, {"/metrics": prometheus_client.make_wsgi_app()}
)

"""
# Logging
# Get the loghandler and rename the field "levelname" to severity
# for correct display of log level in Google Logging
logger = logging.getLogger()
handler = logger.handlers[0]
formatter = jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(threadName) %(module) %(funcName)s %(lineno)d  %(message)s",
    rename_fields={"levelname": "severity"},
)
logger.handlers[0].setFormatter(formatter)
"""

# Metrics dictionary setup
graphs = {}
metrics_key = f"{METRIC_PREFIX}number_of_calls_to_bigquery"
graphs[metrics_key] = prometheus_client.Gauge(
    metrics_key, "The total number of API calls to BigQuery."
)


def generate_random_number() -> None:
    logger.debug('Generating random number.')
    metric_key = f"{METRIC_PREFIX}some_random_number"
    if not metric_key in graphs:
        graphs[metric_key] = prometheus_client.Gauge(
            metric_key,
            "Some random number",
        )
    graphs[metric_key].set(random.random())
    return None


def count_total_and_distinct(
        database="inndata",
        table="v_identifikasjonsnummer",
        column="folkeregisteridentifikator",
) -> None:
    """
    Trigger an API request to BigQuery, where we find:
    * Total number of rows in a table.
    * Unique number of rows in a table based on the uniqueness of a column.

    Then, these metrics are stored as prometheus Gauges within the graphs dictionary.
    """
    # Read from BigQuery
    logger.debug('Submitting count_total_and_uniques query to BigQuery.')
    metric_key = f"{METRIC_PREFIX}number_of_calls_to_bigquery"
    graphs[metric_key].inc()
    result = BQ.count_total_and_uniques(database=database, table=table, column=column)

    # Create and set Prometheus variables
    for key, val in result.items():
        metric_key = f"{METRIC_PREFIX}{key}_rows_in_{table}"
        if not metric_key in graphs:
            graphs[metric_key] = prometheus_client.Gauge(
                metric_key,
                f"The {key} number of rows "
                f"in BigQuery table {GCP_project}.{database}.{table}.",
            )
        graphs[metric_key].set(val)

    return None


def check_valid_and_invalid_fnr(
        database="inndata", table="v_identifikasjonsnummer"
) -> None:
    """
    Check the number of valid fnr and dnr in BigQuery database. If the numbers
    are invalid, then they are categorized as either (prioritized order):
    * 'format' (wrong format, i.e., not 11 digits).
    * 'date' (invalid date, also accounts for dnr).
    * 'control' (invalid control digits, i.e., the two last digits).
    """
    # Read from BigQuery
    logger.debug('Submitting valid_and_invalid_fnr query to BigQuery.')
    metric_key = f"{METRIC_PREFIX}number_of_calls_to_bigquery"
    graphs[metric_key].inc()
    result = BQ.valid_and_invalid_fnr(database=database, table=table)

    # Create and set Prometheus variables
    for key, val in result.items():
        metric_key = f"{METRIC_PREFIX}{key}"
        if not metric_key in graphs:  # (result dict has sufficiently descriptive keys)
            graphs[metric_key] = prometheus_client.Gauge(
                metric_key,
                f"The number of records with {key}"
                f"in BigQuery table {GCP_project}.{database}.{table}.",
            )
        graphs[metric_key].set(val)

    return None


def group_by_and_count(database="inndata", table="v_status", column="status") -> None:
    # Read from BigQuery
    logger.debug('Submitting group_by_and_count query to BigQuery.')
    metric_key = f"{METRIC_PREFIX}number_of_calls_to_bigquery"
    graphs[metric_key].inc()
    result = BQ.group_by_and_count(database=database, table=table, column=column)

    map_group_by_result_to_metric(
        result=result, database=database, table=table, column=column
    )

    return None


def count_hendelsetype() -> None:
    # Read from BigQuery
    logger.debug('Submitting count_hendelsetype query to BigQuery.')
    metric_key = f"{METRIC_PREFIX}number_of_calls_to_bigquery"
    graphs[metric_key].inc()
    result = BQ.count_hendelsetype()

    map_group_by_result_to_metric(
        result=result,
        database="kildedata",
        table="hendelse_persondok",
        column="hendelsetype",
    )

    return None


def map_group_by_result_to_metric(
        result, database="inndata", table="v_status", column="status"
) -> None:
    # Create and set Prometheus variables
    for key, val in result.items():
        metric_key = f"{METRIC_PREFIX}{column}"
        if not metric_key in graphs:
            graphs[metric_key] = prometheus_client.Gauge(
                metric_key,
                f"The number of rows for {column} "
                f"in BigQuery table {GCP_project}.{database}.{table}.",
                ["kode"],
            )
        graphs[metric_key].labels(kode=f"{key}")  # Initialize label
        graphs[metric_key].labels(kode=f"{key}").set(val)

    return None


def get_latest_timestamp(database="kildedata", table="hendelse_persondok") -> None:
    metric_key = f"{METRIC_PREFIX}latest_md_timestamp_{table}"
    if not metric_key in graphs:
        graphs[metric_key] = prometheus_client.Info(
            metric_key,
            "The latest md_timestamp "
            f"in BigQuery table {GCP_project}.{database}.{table}",
        )

    # Read from BigQuery
    logger.debug('Submitting latest_timestamp query to BigQuery.')
    result = BQ.latest_timestamp(database=database, table=table)

    # Result is a dictionary, where key=table, value=latest_timestamp_for_table
    graphs[metric_key].info(result)

    metric_key = f"{METRIC_PREFIX}number_of_calls_to_bigquery"
    graphs[metric_key].inc()

    return None


# Scheduling of function triggers
logger.debug('Configuring job scheduler.')
scheduler = BackgroundScheduler()
# scheduler = BlockingScheduler()

# Count total/unique folkeregisteridentifikator
scheduler.add_job(
    lambda: generate_random_number(),
    "interval",
    name = "generate_random_number",
    **kwargs,
)

# Count total/unique folkeregisteridentifikator
scheduler.add_job(
    lambda: count_total_and_distinct(
        database="inndata",
        table="v_identifikasjonsnummer",
        column="folkeregisteridentifikator",
    ),
    "interval",
    name = "count_total_and_distinct",
    **kwargs,
)

# Count how many with each status
scheduler.add_job(
    lambda: group_by_and_count(database="inndata", table="v_status", column="status"),
    "interval",
    name = "group_by_and_count_status",
    **kwargs,
)

# Count how many with each sivilstand
scheduler.add_job(
    lambda: group_by_and_count(
        database="inndata", table="v_sivilstand", column="sivilstand"
    ),
    "interval",
    name = "group_by_and_count_sivilstand",
    **kwargs,
)

scheduler.add_job(
    lambda: count_hendelsetype(),
    "interval",
    name = "count_hendelsetype",
    **kwargs,
)

# Count how many with each sivilstand
scheduler.add_job(
    lambda: check_valid_and_invalid_fnr(
        database="inndata",
        table="v_identifikasjonsnummer",
    ),
    "interval",
    name = "check_valid_and_invalid_fnr",
    **kwargs,
)

# Latest timestamp
scheduler.add_job(
    get_latest_timestamp,
    "interval",
    name = "get_latest_timestamp",
    **kwargs
)


@app.route("/health/ready")
def ready():
    """Tells whether or not the app is ready to receive requests"""
    return "Ready!"


@app.route("/health/alive")
def alive():
    """Tells whether or not the app is alive"""
    return "Alive!"

@app.route("/")
def app_startup():
    # scheduler.start()
    # atexit.register(lambda: scheduler.shutdown())
    return "Hurray!"


# Start/shutdown
scheduler.start()
atexit.register(lambda: scheduler.shutdown())
