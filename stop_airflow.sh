#!/bin/bash
# Dừng toàn bộ Airflow processes
pkill -f "airflow scheduler" 2>/dev/null && echo "✓ Đã dừng scheduler"
pkill -f "airflow webserver" 2>/dev/null && echo "✓ Đã dừng webserver"
pkill -f "gunicorn"          2>/dev/null
echo "Airflow đã dừng hoàn toàn."
