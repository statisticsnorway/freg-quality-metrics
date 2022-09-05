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
        lambda: metrics.count_total_and_distinct(
            database="inndata",
            table="v_identifikasjonsnummer",
            column="folkeregisteridentifikator",
        ),
        "interval",
        name = "count_total_and_distinct_fnr_inndata",
        **kwargs,
    )

    scheduler.add_job(
        lambda: metrics.count_total_and_distinct(
            database="historikk",
            table="v_identifikasjonsnummer",
            column="folkeregisteridentifikator",
        ),
        "interval",
        name = "count_total_and_distinct_fnr_historikk",
        **kwargs,
    )

    scheduler.add_job(
        lambda: metrics.count_total_and_distinct(
            database="kildedata",
            table="hendelse_persondok",
            column="folkeregisteridentifikator",
        ),
        "interval",
        name = "count_total_and_distinct_fnr_kildedata",
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
        lambda: metrics.check_valid_and_invalid_fnr(
            database="inndata",
            table="v_identifikasjonsnummer",
        ),
        "interval",
        name = "check_valid_and_invalid_fnr_inndata",
        **kwargs,
    )

    scheduler.add_job(
        lambda: metrics.check_valid_and_invalid_fnr(
            database="historikk",
            table="v_identifikasjonsnummer",
        ),
        "interval",
        name = "check_valid_and_invalid_fnr_historikk",
        **kwargs,
    )
    scheduler.add_job(
        lambda: metrics.check_valid_and_invalid_fnr(
            database="kildedata",
            table="hendelse_persondok",
        ),
        "interval",
        name = "check_valid_and_invalid_fnr_kildedata",
        **kwargs,
    )

    # Latest timestamp
    scheduler.add_job(
        metrics.get_latest_timestamp,
        "interval",
        name = "get_latest_timestamp",
        **kwargs
    )

    
    # Start/shutdown
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())
