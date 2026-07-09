"""
DAG: booking_daily_scraper
===========================
Chạy mỗi ngày lúc 08:00 AM (giờ Việt Nam, UTC+7 = 01:00 UTC).

Task 1 — run_scraper:
    Chạy scraper_final.py với ngày check-in = hôm nay, check-out = hôm nay + 2.
    Dữ liệu ghi ra: ~/Desktop/booking_de_project/data/hotels_vietnam_all.jsonl

Task 2 — upload_to_s3 (chạy sau Task 1 thành công):
    Upload hotels_vietnam_all.jsonl lên:
    s3://booking-hotel-data-yourname/raw/hotels.jsonl
    (ghi đè mỗi ngày — cột scrape_date trong file đã ghi rõ ngày cào)

Cách đặt file này:
    ~/Desktop/booking_de_project/airflow/dags/booking_scraper_dag.py
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────────────────────────────────────────────────────────────
# CẤU HÌNH 
# ─────────────────────────────────────────────────────────────────────────────
PROJECT_DIR  = Path("/Users/mac/Desktop/booking_de_project")
SCRAPER_PATH = PROJECT_DIR / "scraper_final.py"
DATA_DIR     = PROJECT_DIR / "data"
S3_BUCKET    = "booking-hotel-data-yourname"
S3_KEY       = "raw/hotels.jsonl"          # đường dẫn cố định, ghi đè mỗi ngày
PYTHON_BIN   = "/Users/mac/Desktop/booking_de_project/venv/bin/python3"          # đường dẫn Python trên macOS
N_CITIES     = 6                           # số thành phố mỗi ngày
PAGES        = 1                           # số trang kết quả mỗi thành phố (~25 khách sạn)

# ─────────────────────────────────────────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner":            "mac",
    "depends_on_past":  False,
    "retries":          1,                      # thử lại 1 lần nếu fail
    "retry_delay":      timedelta(minutes=30),  # chờ 30 phút trước khi thử lại
    "email_on_failure": False,
}

# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — CHẠY SCRAPER
# ─────────────────────────────────────────────────────────────────────────────
def run_scraper(**context) -> None:
    """
    Chạy scraper_final.py với ngày check-in = execution_date của Airflow.
    Airflow truyền execution_date (ngày DAG được schedule) vào context.
    """
    execution_date: datetime = context["execution_date"]

    checkin  = execution_date.strftime("%Y-%m-%d")
    checkout = (execution_date + timedelta(days=2)).strftime("%Y-%m-%d")

    # Tạo thư mục data 
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    cmd = [
        PYTHON_BIN,
        str(SCRAPER_PATH),
        "--checkin",   checkin,
        "--checkout",  checkout,
        "--data-dir",  str(DATA_DIR),
        "--n-cities",  str(N_CITIES),
        "--pages",     str(PAGES),
    ]

    print(f"Chạy lệnh: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(PROJECT_DIR),
    )

    # In log ra Airflow UI để dễ debug
    if result.stdout:
        print("=== STDOUT ===")
        print(result.stdout[-5000:])   # 5000 ký tự cuối tránh log quá dài
    if result.stderr:
        print("=== STDERR ===")
        print(result.stderr[-2000:])

    if result.returncode != 0:
        raise RuntimeError(
            f"Scraper thất bại với exit code {result.returncode}.\n"
            f"Xem log phía trên để biết chi tiết."
        )

    # Kiểm tra file output có tồn tại không
    jsonl_path = DATA_DIR / "hotels_vietnam_all.jsonl"
    if not jsonl_path.exists():
        raise FileNotFoundError(
            f"Scraper chạy xong nhưng không tìm thấy file: {jsonl_path}\n"
            f"Kiểm tra lại --data-dir hoặc log scraper."
        )

    # Đếm số bản ghi để log
    line_count = sum(1 for _ in open(jsonl_path, encoding="utf-8") if _.strip())
    print(f"Scraper hoàn thành — {line_count} bản ghi trong {jsonl_path}")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — UPLOAD LÊN S3
# ─────────────────────────────────────────────────────────────────────────────
def upload_to_s3(**context) -> None:
    """
    Upload hotels_vietnam_all.jsonl lên s3://booking-hotel-data-yourname/raw/hotels.jsonl.
    Dùng AWS CLI thay boto3 để tránh vấn đề credentials trong subprocess Airflow.
    """
    jsonl_path = DATA_DIR / "hotels_vietnam_all.jsonl"

    if not jsonl_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file để upload: {jsonl_path}\n"
            f"Task run_scraper có thể đã fail."
        )

    file_size_mb = jsonl_path.stat().st_size / (1024 * 1024)
    print(f"Upload: {jsonl_path} ({file_size_mb:.1f} MB)")
    print(f"Đích: s3://{S3_BUCKET}/{S3_KEY}")

    result = subprocess.run(
        ["aws", "s3", "cp", str(jsonl_path), f"s3://{S3_BUCKET}/{S3_KEY}"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Upload thất bại:\n{result.stderr}")

    print(f"✓ {result.stdout.strip()}")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 — TRIGGER AWS GLUE ETL (fire-and-forget)
# ─────────────────────────────────────────────────────────────────────────────
GLUE_JOB_NAME = "booking_hotel_etl"

def trigger_glue_etl(**context) -> None:
    """
    Trigger AWS Glue job bằng AWS CLI — fire-and-forget.
    Glue chạy độc lập, không cần chờ kết quả.
    Kiểm tra kết quả trên AWS Glue Console hoặc CloudWatch.
    """
    result = subprocess.run(
        ["aws", "glue", "start-job-run",
         "--job-name", GLUE_JOB_NAME,
         "--region", "ap-southeast-1"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Không thể trigger Glue job:\n{result.stderr}"
        )

    print(f"Glue job triggered: {result.stdout.strip()}")


# ─────────────────────────────────────────────────────────────────────────────
# ĐỊNH NGHĨA DAG
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="booking_daily_scraper",
    default_args=default_args,
    description="Cào khách sạn Việt Nam từ Booking.com và upload lên S3 mỗi ngày",
    # Chạy lúc 08:00 AM giờ Việt Nam = 01:00 UTC
    # Cú pháp: "phút giờ * * *"
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 4, 16),   # ngày bắt đầu — Airflow không backfill trước ngày này
    catchup=False,                       # không chạy bù các ngày đã qua
    max_active_runs=1,                   # chỉ 1 run tại một thời điểm, tránh chạy chồng
    tags=["booking", "scraping", "vietnam"],
) as dag:

    task_scrape = PythonOperator(
        task_id="run_scraper",
        python_callable=run_scraper,
        execution_timeout=timedelta(hours=3),
    )

    task_upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        execution_timeout=timedelta(minutes=10),
    )

    task_glue = PythonOperator(
        task_id="trigger_glue_etl",
        python_callable=trigger_glue_etl,
        execution_timeout=timedelta(hours=2),
    )

    # Task 1 → Task 2 → Task 3
    task_scrape >> task_upload >> task_glue