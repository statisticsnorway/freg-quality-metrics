import logging.config

from fastapi import FastAPI
from pythonjsonlogger import jsonlogger
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI()

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

# Metrics
Instrumentator().instrument(app).expose(app)


@app.get("/health/ready", status_code=200)
def ready():
    """Tells whether or not the app is ready to receive requests"""
    return "Ready!"


@app.get("/health/alive", status_code=200)
def alive():
    """Tells whether or not the app is alive"""
    return "Alive!"


@app.on_event("startup")
def app_startup():
    """Does some initial startup stuff"""
    logger.info("Doing som startup stuff...")
