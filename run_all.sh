#!/bin/bash
#Stop errors
set -e

echo "Running the scripts . . . . "

python3 fetch_data.py
python3 make_map.py
python3 generate_risk_map.py
python3 analyse_weather.py
python3 super_script.py

echo "Reports generated!"
