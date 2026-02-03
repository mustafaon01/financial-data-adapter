"""Airflow DAG for sync jobs."""

import os
import time
from datetime import datetime, timedelta

import requests
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from airflow import DAG

TENANTS = ["BANK001", "BANK002", "BANK003"]
LOAN_TYPES = ["RETAIL", "COMMERCIAL"]

POLL_INTERVAL_SEC = 5
POLL_TIMEOUT_SEC = 10 * 60


def get_settings():
    base_url = Variable.get(
        "FDA_API_BASE_URL", default_var=os.getenv("FDA_API_BASE_URL", "http://api:8000")
    ).rstrip("/")
    username = Variable.get(
        "FDA_SYNC_USERNAME", default_var=os.getenv("FDA_SYNC_USERNAME", "admin")
    )
    password = Variable.get(
        "FDA_SYNC_PASSWORD", default_var=os.getenv("FDA_SYNC_PASSWORD", "admin123")
    )
    return base_url, username, password


def trigger_sync(tenant_id, loan_type, **context):
    base_url, username, password = get_settings()
    url = base_url + "/api/sync/"
    payload = {"tenant_id": tenant_id, "loan_type": loan_type}

    r = requests.post(url, json=payload, auth=(username, password), timeout=60)
    if r.status_code not in (200, 201, 202):
        raise AirflowFailException(
            f"Sync failed {tenant_id}/{loan_type}: {r.status_code} {r.text}"
        )

    data = r.json()

    batch_id = data.get("id") or data.get("batch_id")
    if not batch_id:
        raise AirflowFailException(
            f"batch_id not returned for {tenant_id}/{loan_type}. response={data}"
        )

    context["ti"].xcom_push(key="batch_id", value=str(batch_id))
    return str(batch_id)


def wait_batch(tenant_id, loan_type, **context):
    base_url, username, password = get_settings()

    batch_id = context["ti"].xcom_pull(key="batch_id")
    if not batch_id:
        raise AirflowFailException(
            f"Missing batch_id in XCom for {tenant_id}/{loan_type}"
        )

    url = base_url + f"/api/batches/{batch_id}/"

    start = time.time()
    last = None

    while True:
        if time.time() - start > POLL_TIMEOUT_SEC:
            raise AirflowFailException(f"Batch timeout: {batch_id} last={last}")

        r = requests.get(url, auth=(username, password), timeout=60)
        if r.status_code != 200:
            raise AirflowFailException(
                f"Batch status fetch failed {batch_id}: {r.status_code} {r.text}"
            )

        last = r.json()
        status = (last.get("status") or "").upper()

        if status == "SUCCESS":
            return last

        if status in ("FAILED", "FAILED_VALIDATION"):
            msg = last.get("error_message")
            # İstersen errors'ı da çekip loglayabiliriz
            raise AirflowFailException(
                f"Batch failed: {batch_id} status={status} msg={msg}"
            )

        time.sleep(POLL_INTERVAL_SEC)


default_args = {
    "owner": "fda",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="fda_auto_sync",
    description="Triggers /api/sync for all tenants & loan types and waits for completion",
    start_date=datetime(2026, 2, 1),
    schedule="*/10 * * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["financial-data-adapter"],
) as dag:
    for t in TENANTS:
        for lt in LOAN_TYPES:
            trig = PythonOperator(
                task_id=f"trigger_{t.lower()}_{lt.lower()}",
                python_callable=trigger_sync,
                op_kwargs={"tenant_id": t, "loan_type": lt},
            )

            wait = PythonOperator(
                task_id=f"wait_{t.lower()}_{lt.lower()}",
                python_callable=wait_batch,
                op_kwargs={"tenant_id": t, "loan_type": lt},
            )

            trig >> wait
