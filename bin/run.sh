#!/usr/bin/env bash

uwsgi --http 0.0.0.0:8080 --wsgi-file app.py --master --callable app --enable-threads --uid aurora
