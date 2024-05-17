#!/usr/bin/env bash

python3 ./bench.py "$@"
python3 ./bench-long-queue.py

python3 ./plot-map.py "$@"
python3 ./plot-queue.py "$@"
python3 ./plot-long-queue.py
