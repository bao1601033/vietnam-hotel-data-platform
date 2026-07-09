#!/bin/bash
# Khởi động Airflow — chạy file này mỗi khi muốn dùng Airflow UI
# Dừng bằng: Ctrl+C trong terminal, sau đó chạy: bash stop_airflow.sh

export AIRFLOW_HOME="/Users/mac/Desktop/booking_de_project/airflow"
source "/Users/mac/Desktop/booking_de_project/venv_airflow/bin/activate"

echo "Khởi động Airflow..."
echo "Mở trình duyệt: http://localhost:8080"
echo "Username: admin | Password: admin123"
echo "Nhấn Ctrl+C để dừng"
echo ""

# Chạy scheduler (tự động trigger DAG) + webserver (UI) song song
airflow scheduler &
SCHEDULER_PID=$!

airflow webserver --port 8080 &
WEBSERVER_PID=$!

# Chờ Ctrl+C
trap "kill $SCHEDULER_PID $WEBSERVER_PID 2>/dev/null; echo 'Đã dừng Airflow.'" INT
wait
