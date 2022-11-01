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


def preagg_total_and_distinct() -> None:
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
    df = BQ.pre_aggregate_total_and_uniques()

    # Create and set Prometheus variables
    metric_total = f"{METRIC_PREFIX}total_rows"
    metric_unique = f"{METRIC_PREFIX}unique_rows"

    for i, row in df.iterrows():
        if not metric_total in graphs:
            graphs[metric_total] = prometheus_client.Gauge(
                metric_total,
                f"The total number of rows",
                ["database", "table", "column"]
            )
            graphs[metric_total].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}")  # Initialize label
        graphs[metric_total].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}").set(row.totalt)
        if not metric_unique in graphs:
            graphs[metric_unique] = prometheus_client.Gauge(
                metric_unique,
                f"The unique number of rows",
                ["database", "table", "column"]
            )
            graphs[metric_unique].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}")  # Initialize label
        graphs[metric_unique].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}").set(row.distinkte)

    end = datetime.datetime.now()
    metrics_time_used(f"preagg_total_and_distinct", "kvalitet", "metrics_count_total_and_distinct", "", start, end)
    return None


def preagg_valid_and_invalid_idents() -> None:
    """
    Check the number of valid fnr and dnr in BigQuery database. If the numbers
    are invalid, then they are categorized as either (prioritized order):
    * 'format' (wrong format, i.e., not 11 digits).
    * 'date' (invalid date, also accounts for dnr).
    * 'control' (invalid control digits, i.e., the two last digits).
    * 'digit' (invalid first digit, fnr > 3, dnr < 4)
    """
    # Read from BigQuery
    logger.debug('Submitting valid_and_invalid_fnr query to BigQuery.')
    start = datetime.datetime.now()
    metrics_count_calls()
    df = BQ.pre_aggregated_valid_fnr()

    # Create and set Prometheus variables
    metric_total = f"{METRIC_PREFIX}ident_total"
    metric_format = f"{METRIC_PREFIX}ident_invalid_format"
    metric_digit = f"{METRIC_PREFIX}ident_invalid_first_digit"
    metric_date = f"{METRIC_PREFIX}ident_invalid_date"
    metric_control = f"{METRIC_PREFIX}ident_invalid_control_digit"

    for i, row in df.iterrows():
        if not metric_total in graphs:
            graphs[metric_total] = prometheus_client.Gauge(metric_total, f"The number of records with identa by type ", ["database", "table", "column", "type"])
            graphs[metric_total].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="fnr")  # Initialize label
        if not metric_format in graphs:
            graphs[metric_format] = prometheus_client.Gauge(metric_format, f"Idents with invalid format ", ["database", "table", "column", "type"])
            graphs[metric_format].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="fnr")  # Initialize label
        if not metric_digit in graphs:
            graphs[metric_digit] = prometheus_client.Gauge(metric_digit, f"Idents with invalid first digit ", ["database", "table", "column", "type"])
            graphs[metric_digit].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="fnr")  # Initialize label
        if not metric_date in graphs:
            graphs[metric_date] = prometheus_client.Gauge(metric_date, f"Idents with invalide date ", ["database", "table", "column", "type"])
            graphs[metric_date].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="fnr")  # Initialize label
        if not metric_control in graphs:
            graphs[metric_control] = prometheus_client.Gauge(metric_control, f"Idents with invalid control digits ", ["database", "table", "column", "type"])
            graphs[metric_control].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="fnr")  # Initialize label
        graphs[metric_total].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="fnr").set(row.fnr_total_count)
        graphs[metric_total].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="dnr").set(row.dnr_total_count)
        graphs[metric_format].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="fnr").set(row.fnr_invalid_format)
        graphs[metric_format].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="dnr").set(row.dnr_invalid_format)
        graphs[metric_digit].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="fnr").set(row.fnr_invalid_first_digit)
        graphs[metric_digit].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="dnr").set(row.dnr_invalid_first_digit)
        graphs[metric_date].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="fnr").set(row.fnr_invalid_date)
        graphs[metric_date].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="dnr").set(row.dnr_invalid_date)
        graphs[metric_control].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="fnr").set(row.fnr_invalid_control)
        graphs[metric_control].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}", type="dnr").set(row.dnr_invalid_control)

    end = datetime.datetime.now()
    metrics_time_used("preagg_valid_and_invalid_idents", "kvalitet", "metrics_count_valid_fnr_dnr", "", start, end)
    return None


def preagg_group_by_and_count() -> None:
    logger.debug('Submitting preagg_group_by_and_count query to BigQuery.')
    start = datetime.datetime.now()
    metrics_count_calls()
    metric_key = f"{METRIC_PREFIX}group_by"

    df = BQ.pre_aggregated_count_group_by()
    for i, row in df.iterrows():
        if not metric_key in graphs:
            graphs[metric_key] = prometheus_client.Gauge(
                metric_key,
                f"The number of rows by group",
                ["group", "database", "table", "column"]
            )
            graphs[metric_key].labels(group=f"{row.gruppe}", database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}")  # Initialize label
        graphs[metric_key].labels(group=f"{row.gruppe}", database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}").set(row.antall)

    end = datetime.datetime.now()
    metrics_time_used(f"preagg_group_by_and_count", "kvalitet", "metrics_count_group_by", "", start, end)
    return None


def preagg_num_citizenships() -> None:
    logger.debug('Submitting preagg_num_citizenships query to BigQuery.')
    start = datetime.datetime.now()
    metrics_count_calls()
    metric_key = f"{METRIC_PREFIX}ant_statsborgerskap"

    df = BQ.pre_aggregated_number_of_citizenships()
    for i, row in df.iterrows():
        if not metric_key in graphs:
            graphs[metric_key] = prometheus_client.Gauge(
                metric_key,
                f"The number of persons with multiple citizenships",
                ["group", "database"]
            )
            graphs[metric_key].labels(group=f"{row.gruppe}", database=f"{row.datasett}")  # Initialize label
        graphs[metric_key].labels(group=f"{row.gruppe}", database=f"{row.datasett}").set(row.antall)

    end = datetime.datetime.now()
    metrics_time_used(f"preagg_num_citizenships", "kvalitet", "metrics_antall_statsborgerskap", "", start, end)
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


def preagg_latest_timestamp() -> None:
    logger.debug(f"Submitting pre_aggregated_latest_timestamp ")
    start = datetime.datetime.now()
    metrics_count_calls()
    metric_key = f"{METRIC_PREFIX}latest_timestamp"

    df = BQ.pre_aggregated_latest_timestamp()
    for i, row in df.iterrows():
        if not metric_key in graphs:
            graphs[metric_key] = prometheus_client.Info(
                metric_key,
                "The latest timestamp ",
                ["database", "table", "column"]
            )
            graphs[metric_key].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}")  # Initialize label
        result = {} # Info-metric needs key-value
        result["timestamp"] = row.latest_timestamp
        graphs[metric_key].labels(database=f"{row.datasett}", table=f"{row.tabell}", column=f"{row.variabel}").info(result)

    end = datetime.datetime.now()
    metrics_time_used("preagg_latest_timestamp", "kvalitet", "v_latest_timestamp", "", start, end)
    return None

