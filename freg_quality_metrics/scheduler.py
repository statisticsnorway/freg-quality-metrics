import atexit
import logging


logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")


from apscheduler.schedulers.background import BackgroundScheduler

from . import metrics


def configure_scheduler(**kwargs):

    # Scheduling of function triggers
    logger.debug("Configuring job scheduler.")
    scheduler = BackgroundScheduler()

    # Count total/unique folkeregisteridentifikator
    scheduler.add_job(
        lambda: metrics.preagg_total_and_distinct(),
        "interval",
        name="preagg_total_and_distinct",
        **kwargs,
    )

    # Count how many with each status
    scheduler.add_job(
        lambda: metrics.preagg_group_by_and_count(),
        "interval",
        name="preagg_group_by_and_count",
        **kwargs,
    )

    scheduler.add_job(
        lambda: metrics.preagg_valid_and_invalid_idents(),
        "interval",
        name="preagg_valid_and_invalid_idents",
        **kwargs,
    )

    scheduler.add_job(
        lambda: metrics.preagg_latest_timestamp(),
        "interval",
        name="preagg_latest_timestamp",
        **kwargs,
    )

    scheduler.add_job(
        lambda: metrics.preagg_num_citizenships(),
        "interval",
        name="preagg_num_citizenships",
        **kwargs,
    )

    # ---
    # DSF SITUASJONSUTTAK
    scheduler.add_job(
        lambda: metrics.dsfsit_latest_timestamp(),
        "interval",
        name="dsfsit_latest_timestamp",
        **kwargs,
    )

    scheduler.add_job(
        lambda: metrics.dsfsit_qa_nullvals_latest(),
        "interval",
        name="dsfsit_qa_nullvals_latest",
        **kwargs,
    )

    scheduler.add_job(
        lambda: metrics.dsfsit_qa_nullvals_diff(),
        "interval",
        name="dsfsit_qa_nullvals_diff",
        **kwargs,
    )

    # Start/shutdown
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())
