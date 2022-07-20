cd app
poetry run gunicorn main:app -b 127.0.0.1:8080 -w 1 -k uvicorn.workers.UvicornWorker -t 0 --log-config logging.config --log-level debug
