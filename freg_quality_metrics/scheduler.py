import atexit
import logging
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")


from . import metrics 

from apscheduler.schedulers.background import BackgroundScheduler


def configure_scheduler(**kwargs):    

    # Scheduling of function triggers
    logger.debug('Configuring job scheduler.')
    scheduler = BackgroundScheduler()

    # Count total/unique folkeregisteridentifikator
    scheduler.add_job(
        lambda: metrics.count_total_and_distinct_identifikasjonsnummer(),
        "interval",
        name = "count_total_and_distinct_identifikasjonsnummer",
        **kwargs,
    )

    # Count how many with each status
    scheduler.add_job(
        lambda: metrics.group_by_and_count(database="inndata", table="v_status", column="status"),
        "interval",
        name = "group_by_and_count_status",
        **kwargs,
    )

    # Count how many with each sivilstand
    scheduler.add_job(
        lambda: metrics.group_by_and_count(
            database="inndata", table="v_sivilstand", column="sivilstand"
        ),
        "interval",
        name = "group_by_and_count_sivilstand",
        **kwargs,
    )

    scheduler.add_job(
        lambda: metrics.count_hendelsetype(),
        "interval",
        name = "count_hendelsetype",
        **kwargs,
    )

    scheduler.add_job(
        lambda: metrics.check_valid_and_invalid_idents(),
        "interval",
        name = "check_valid_and_invalid_idents",
        **kwargs,
    )

    # Latest timestamp
    scheduler.add_job(
        lambda: metrics.get_latest_timestamps(),
        "interval",
        name = "get_latest_timestamps",
        **kwargs
    )

    scheduler.add_job(
        lambda: metrics.count_statsborgerskap(),
        "interval",
        name = "count_statsborgerskap",
        **kwargs,
    )

    # Count how many with each gender
    scheduler.add_job(
        lambda: metrics.group_by_and_count(
            database="inndata", table="v_kjoenn", column="kjoenn"
        ),
        "interval",
        name = "group_by_and_count_kjoenn",
        **kwargs,
    )

    # Start/shutdown
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())
