import logging
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")

from flask import Flask

import datetime
import prometheus_client
from .config import METRIC_PREFIX, GCP_PROJECT

from .bigquery import BigQuery
from werkzeug.middleware.dispatcher import DispatcherMiddleware


# Metrics dictionary setup
graphs = {}
graphs["freg_metrics_interval"] = prometheus_client.Gauge("freg_metrics_interval", "Interval of metrics scheduler")

BQ = BigQuery(GCP_project=GCP_PROJECT)

def configure_prometheus(app: Flask, **kwargs):

    # csrf = CSRFProtect(app)
    logger.debug('Setting up prometheus client.')
    app.wsgi_app = DispatcherMiddleware(
        app.wsgi_app, {"/metrics": prometheus_client.make_wsgi_app()}
    )
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
        graphs[metric_key].labels(name=f"{metricname}", database=f"{database}", table=f"{table}", column=f"{column}")  # Initialize label

    diff = end - start
    sec = diff.total_seconds()
    graphs[metric_key].labels(name=f"{metricname}", database=f"{database}", table=f"{table}", column=f"{column}").set(sec)
    return None


def count_total_and_distinct_identifikasjonsnummer() -> None:
    count_total_and_distinct(database="inndata", table="v_identifikasjonsnummer", column="folkeregisteridentifikator",)
    count_total_and_distinct(database="historikk", table="v_identifikasjonsnummer", column="folkeregisteridentifikator",)
    count_total_and_distinct(database="kildedata", table="hendelse_persondok", column="folkeregisteridentifikator",)
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


def check_valid_and_invalid_idents() -> None:
    check_valid_and_invalid_fnr(database="inndata", table="v_identifikasjonsnummer",)
    check_valid_and_invalid_fnr(database="historikk", table="v_identifikasjonsnummer",)
    check_valid_and_invalid_fnr(database="kildedata", table="hendelse_persondok",)
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
                f"The number of records with {key} ",
                ["database", "table"]
            )
            graphs[metric_key].labels(database=f"{database}", table=f"{table}")  # Initialize label
        graphs[metric_key].labels(database=f"{database}", table=f"{table}").set(val)

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


def get_latest_timestamps() -> None:
    get_latest_timestamp(database="kildedata", table="hendelse_persondok", column="md_timestamp", parse_format="%d-%m-%Y %H:%M:%S")
    get_latest_timestamp(database="kildedata", table="hendelse_persondok", column="ajourholdstidspunkt", parse_format="%b %e, %Y, %I:%M:%S %p")
    get_latest_timestamp(database="inndata", table="v_identifikasjonsnummer", column="ajourholdstidspunkt", parse_format="%Y-%m-%dT%H:%M:%E*S%Ez")
    get_latest_timestamp(database="historikk", table="v_identifikasjonsnummer", column="ajourholdstidspunkt", parse_format="%Y-%m-%dT%H:%M:%E*S%Ez")
    return None


def get_latest_timestamp(database="kildedata", table="hendelse_persondok", column="md_timestamp", parse_format="%d-%m-%Y %H:%M:%S") -> None:
    metric_key = f"{METRIC_PREFIX}latest_timestamp"
    if not metric_key in graphs:
        graphs[metric_key] = prometheus_client.Info(
            metric_key,
            "The latest timestamp ",
            ["database", "table", "column"]
        )
        graphs[metric_key].labels(database=f"{database}", table=f"{table}", column=f"{column}")  # Initialize label

    # Read from BigQuery
    logger.debug(f"Submitting latest_timestamp query for {database}.{table}.{column}")
    start = datetime.datetime.now()
    metrics_count_calls()
    result = BQ.latest_timestamp(database=database, table=table, column=column, parse_format=parse_format)

    # Result is a dictionary, where key=table, value=latest_timestamp_for_table
    graphs[metric_key].labels(database=f"{database}", table=f"{table}", column=f"{column}").info(result)

    end = datetime.datetime.now()
    metrics_time_used("get_latest_timestamp", database, table, column, start, end)
    return None


def count_statsborgerskap() -> None:
    # Read from BigQuery
    logger.debug('Submitting count_statsborgerskap query to BigQuery.')
    start = datetime.datetime.now()
    metrics_count_calls()
    result = BQ.count_statsborgerskap()

    database="klargjort"
    table="v_avled_statsborgerskap"
    column="statsborgerskap_x"

    map_group_by_result_to_metric(
        result=result,
        database=database,
        table=table,
        column=column,
    )

    end = datetime.datetime.now()
    metrics_time_used("count", database, table, column, start, end)
    return None
