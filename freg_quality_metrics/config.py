import os


# Configure application path
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
METRIC_PREFIX = "freg_"
GCP_PROJECT = os.environ.get("GCP_PROJECT", "dev-freg-3896")
INTERVAL_MINUTES = os.environ.get("INTERVAL_MINUTES", "5")


def configure_logging():
    # Configure logging
    import logging.config

    logging.config.fileConfig(os.path.join(MODULE_DIR, "logging.config"))
    logger = logging.getLogger(__name__)
    logger.debug("Logging is configured.")
