#!/usr/bin/env bash

python3 ./bench-scripts/immut/bench.py
python3 ./bench-scripts/immut/bench-short-lists.py
python3 ./bench-scripts/immut/bench-hp-trees.py

python3 ./bench-scripts/immut/plot.py
python3 ./bench-scripts/immut/plot-short-lists.py
python3 ./bench-scripts/immut/plot-hp-trees.py
