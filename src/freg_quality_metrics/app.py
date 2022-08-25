# Scheduled triggering of functions
import atexit

from freg_quality_metrics import config

import logging
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")


# from pythonjsonlogger import jsonlogger

# Prometheus utilities
import os
import prometheus_client

# Local class for calling our BigQuery databases
from freg_quality_metrics.api.bigquery import BigQuery
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


# Metrics dictionary setup
graphs = {}
graphs["freg_metrics_interval"] = prometheus_client.Gauge("freg_metrics_interval", "Interval of metrics scheduler")
graphs["freg_metrics_interval"].set(kwargs["minutes"])


def metrics_count_calls() -> None:
    metric_key = f"{METRIC_PREFIX}metrics_calls_to_bigquery"
    if not metric_key in graphs:
        graphs[metric_key] = prometheus_client.Gauge(
            metric_key, "The total number of calls to BigQuery."
        )
    graphs[metric_key].inc()
    return None


def metrics_time_used(metricname, database, table, column, start=datetime.datetime.now(), end=datetime.datetime.now()) -> None:
    metric_key = f"{METRIC_PREFIX}metrics_time_used"
    if not metric_key in graphs:
        graphs[metric_key] = prometheus_client.Gauge(
            metric_key,
            "Time used to generate metric",
            ["name", "database", "table", "column"]
        )
    diff = end - start
    sec = diff.total_seconds()
    graphs[metric_key].labels(name=f"{metricname}", database=f"{database}", table=f"{table}", column=f"{column}")  # Initialize label
    graphs[metric_key].labels(name=f"{metricname}", database=f"{database}", table=f"{table}", column=f"{column}").set(sec)
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
    start = datetime.datetime.now()
    metrics_count_calls()
    result = BQ.count_total_and_uniques(database=database, table=table, column=column)

    # Create and set Prometheus variables
    for key, val in result.items():
        metric_key = f"{METRIC_PREFIX}{key}_rows"
        if not metric_key in graphs:
            graphs[metric_key] = prometheus_client.Gauge(
                metric_key,
                f"The {key} number of rows",
                ["database", "table", "column"]
            )
        graphs[metric_key].labels(database=f"{database}", table=f"{table}", column=f"{column}")  # Initialize label
        graphs[metric_key].labels(database=f"{database}", table=f"{table}", column=f"{column}").set(val)

    end = datetime.datetime.now()
    metrics_time_used(f"count_total_and_distinct", database, table, column, start, end)
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
    start = datetime.datetime.now()
    metrics_count_calls()
    result = BQ.valid_and_invalid_fnr(database=database, table=table)

    # Create and set Prometheus variables
    for key, val in result.items():
        metric_key = f"{METRIC_PREFIX}{key}"
        if not metric_key in graphs:  # (result dict has sufficiently descriptive keys)
            graphs[metric_key] = prometheus_client.Gauge(
                metric_key,
                f"The number of records with {key} "
                f"in BigQuery table {GCP_project}.{database}.{table}.",
            )
        graphs[metric_key].set(val)

    end = datetime.datetime.now()
    metrics_time_used("check_valid_and_invalid", database, table, "folkeregisteridentifikator", start, end)
    return None


def group_by_and_count(database="inndata", table="v_status", column="status") -> None:
    # Read from BigQuery
    logger.debug('Submitting group_by_and_count query to BigQuery.')
    start = datetime.datetime.now()
    metrics_count_calls()
    result = BQ.group_by_and_count(database=database, table=table, column=column)

    map_group_by_result_to_metric(
        result=result, database=database, table=table, column=column
    )

    end = datetime.datetime.now()
    metrics_time_used(f"group_by_and_count", database, table, column, start, end)
    return None


def count_hendelsetype() -> None:
    # Read from BigQuery
    logger.debug('Submitting count_hendelsetype query to BigQuery.')
    start = datetime.datetime.now()
    metrics_count_calls()
    result = BQ.count_hendelsetype()

    database="kildedata"
    table="hendelse_persondok"
    column="hendelsetype"

    map_group_by_result_to_metric(
        result=result,
        database=database,
        table=table,
        column=column,
    )

    end = datetime.datetime.now()
    metrics_time_used("count", database, table, column, start, end)
    return None


def map_group_by_result_to_metric(
        result, database="inndata", table="v_status", column="status"
) -> None:
    # Create and set Prometheus variables
    for key, val in result.items():
        metric_key = f"{METRIC_PREFIX}group_by"
        if not metric_key in graphs:
            graphs[metric_key] = prometheus_client.Gauge(
                metric_key,
                f"The number of rows by group",
                ["group", "database", "table", "column"]
        )
        graphs[metric_key].labels(group=f"{key}", database=f"{database}", table=f"{table}", column=f"{column}")  # Initialize label
        graphs[metric_key].labels(group=f"{key}", database=f"{database}", table=f"{table}", column=f"{column}").set(val)

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
    start = datetime.datetime.now()
    metrics_count_calls()
    result = BQ.latest_timestamp(database=database, table=table)

    # Result is a dictionary, where key=table, value=latest_timestamp_for_table
    graphs[metric_key].info(result)

    end = datetime.datetime.now()
    metrics_time_used("get_latest_timestamp", database, table, "", start, end)
    return None


# Scheduling of function triggers
logger.debug('Configuring job scheduler.')
scheduler = BackgroundScheduler()


# Count total/unique folkeregisteridentifikator
scheduler.add_job(
    lambda: count_total_and_distinct(
        database="inndata",
        table="v_identifikasjonsnummer",
        column="folkeregisteridentifikator",
    ),
    "interval",
    name = "count_total_and_distinct_fnr",
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

"""
scheduler.add_job(
    lambda: check_valid_and_invalid_fnr(
        database="inndata",
        table="v_identifikasjonsnummer",
    ),
    "interval",
    name = "check_valid_and_invalid_fnr",
    **kwargs,
)
"""

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
    return Response(status=200)


@app.route("/health/alive")
def alive():
    """Tells whether or not the app is alive"""
    return Response(status=200)


@app.route("/")
def app_startup():
    # scheduler.start()
    # atexit.register(lambda: scheduler.shutdown())
    return "Hurray!"


# Start/shutdown
scheduler.start()
atexit.register(lambda: scheduler.shutdown())
