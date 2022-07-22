# Scheduled triggering of functions
import atexit

# import logging.config
# from pythonjsonlogger import jsonlogger

# Prometheus utilities
import random

import prometheus_client

# Local class for calling our BigQuery databases
from api.bigquery import BigQuery
from apscheduler.schedulers.background import BackgroundScheduler

# Flask (webapp library) and flask-related dispatcher
from flask import Flask, Response
# from flask_wtf.csrf import CSRFProtect
from werkzeug.middleware.dispatcher import DispatcherMiddleware

# Scheduler: keyword arguments (how often to trigger)
kwargs = {"minutes": 1}

# Global variables
GCP_project = "dev-freg-3896"
# BQ = BigQuery(GCP_project=GCP_project)
METRIC_PREFIX = "freg_"

# Flask setup
app = Flask(__name__)
# csrf = CSRFProtect(app)
app.wsgi_app = DispatcherMiddleware(
    app.wsgi_app, {"/metrics": prometheus_client.make_wsgi_app()}
)
"""
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
"""

# Metrics dictionary setup
graphs = {}
metric_key = f"{METRIC_PREFIX}some_random_number"
graphs[metric_key] = prometheus_client.Gauge(
    metric_key, "Some random number")


def generate_random_number() -> None:
    mkey = f"{METRIC_PREFIX}some_random_number"
    graphs[mkey].set(random.random())
    return None


# Scheduling of function triggers
scheduler = BackgroundScheduler()

# Count total/unique folkeregisteridentifikator
scheduler.add_job(
    lambda: generate_random_number(),
    "interval",
    **kwargs,
)


@app.route("/health/ready")
def ready():
    """Tells whether or not the app is ready to receive requests"""
    return "Ready!"


@app.route("/health/alive")
def alive():
    """Tells whether or not the app is alive"""
    return "Alive!"

"""
@app.route("/")
def app_startup():
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())
    return "Hurray!"
"""

# Start/shutdown
scheduler.start()
atexit.register(lambda: scheduler.shutdown())
