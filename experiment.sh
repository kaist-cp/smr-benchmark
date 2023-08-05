#!/usr/bin/env bash

echo "======================================================================="
echo "1. Throughputs & unreclaimed memory blocks on varying ratio of writes"
echo "======================================================================="

python3 ./bench-scripts/bench.py
python3 ./bench-scripts/plot.py

echo "======================================================================="
echo "2. Throughputs on a long-running operations"
echo "======================================================================="

python3 ./bench-scripts/bench-long-running.py
python3 ./bench-scripts/plot-long-running.py
