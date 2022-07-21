#!/usr/bin/env bash

uwsgi --http 127.0.0.1:8000 --wsgi-file app.py --callable app --enable-threads --uid aurora
