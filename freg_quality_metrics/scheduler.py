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
        lambda: metrics.preagg_group_by_and_count(),
        "interval",
        name = "preagg_group_by_and_count",
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
        lambda: metrics.get_latest_timestamp(),
        "interval",
        name = "get_latest_timestamp",
       **kwargs
    )

    scheduler.add_job(
        lambda: metrics.count_statsborgerskap(),
        "interval",
        name = "count_statsborgerskap",
        **kwargs,
    )


    # Start/shutdown
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())
