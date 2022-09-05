import logging
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")


# Local class for calling our BigQuery databases
from . import metrics, scheduler

# Flask (webapp library) and flask-related dispatcher
from flask import Flask, Response
# from flask_wtf.csrf import CSRFProtect

import datetime

def create_app():
    # Scheduler: keyword arguments (how often to trigger)
    kwargs = {"minutes": 5, "next_run_time": datetime.datetime.now()}

    # Environment variables

    # Flask setup
    logger.info('Initialising Flask app.')
    app = Flask(__name__)
    
    metrics.configure_prometheus(app, **kwargs)
    scheduler.configure_scheduler(**kwargs)

    @app.route("/health/ready")
    def ready():
        """Tells whether or not the app is ready to receive requests"""
        return Response(status=200)


    @app.route("/health/alive")
    def alive():
        """Tells whether or not the app is alive"""
        return Response(status=200)


    @app.route("/")
    def app_startup():
        # scheduler.start()
        # atexit.register(lambda: scheduler.shutdown())
        return "Welcome"

    return app