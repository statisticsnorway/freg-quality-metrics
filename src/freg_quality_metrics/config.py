import os

# Configure application path
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

# Configure logging
import logging
import logging.config
logging.config.fileConfig(os.path.join(MODULE_DIR,'logging.config'))
logger = logging.getLogger(__name__)
logger.debug('Logging is configured.')