#!/usr/bin/env bash

LOG_FILE="logs/$(date +"%d_%m_%Y:%k:%M:%S").txt"

python3 main.py -t employee -use_json employee.json -std 2012-01-02 -ed 2012-06-01> "$LOG_FILE"