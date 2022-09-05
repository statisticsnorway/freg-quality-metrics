#!/usr/bin/env bash

uwsgi --http 0.0.0.0:8080 --wsgi-file run.py --callable app --enable-threads --uid aurora
