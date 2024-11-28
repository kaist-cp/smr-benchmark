#!/usr/bin/env bash

python3 ./bench-scripts/hp-revisited/bench.py
python3 ./bench-scripts/hp-revisited/bench-short-lists.py
python3 ./bench-scripts/hp-revisited/bench-hp-trees.py

python3 ./bench-scripts/hp-revisited/plot.py
python3 ./bench-scripts/hp-revisited/plot-short-lists.py
python3 ./bench-scripts/hp-revisited/plot-hp-trees.py
