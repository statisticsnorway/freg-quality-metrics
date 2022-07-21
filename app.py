# Scheduled triggering of functions
# import atexit

# Prometheus utilities
import random

import prometheus_client

# Local class for calling our BigQuery databases
from api.bigquery import BigQuery
# from apscheduler.schedulers.background import BackgroundScheduler

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

# Prometheus setup
graphs = {}
metric_key = f"{METRIC_PREFIX}number_of_calls_to_bigquery"
graphs[metric_key] = prometheus_client.Gauge(
    metric_key, "The total number of API calls to BigQuery.")
graphs[metric_key].set(random.random())


@app.route("/health/ready")
def ready():
    """Tells whether or not the app is ready to receive requests"""
    return "Ready!"


@app.route("/health/alive")
def alive():
    """Tells whether or not the app is alive"""
    return "Alive!"


@app.route("/")
def app_startup():
    """Does some initial startup stuff"""
    return "Hurray!"
# logger.info("Doing som startup stuff...")
