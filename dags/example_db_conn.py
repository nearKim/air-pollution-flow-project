import os
from datetime import datetime, timedelta
from time import sleep

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import text

from infra.db import engine


def use_connection(*args, **kwargs):
    with engine.connect() as conn:
        conn.execute(text("select 1 from air_pollution.air_quality;"))
        print(f"selecting from {os.getpid()} \n", flush=True)
        conn.execute(text("select sleep(60);"))
        print(f"sleeping from {os.getpid()} \n", flush=True)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "example_db_conn",
    default_args=default_args,
    description="db conn test",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=True,
    max_active_runs=70,
    max_active_tasks=100,
    tags=["example"],
) as dag:
    start = DummyOperator(task_id="start")
    t1 = PythonOperator(task_id="use_conn", python_callable=use_connection, task_concurrency=70)
    end = DummyOperator(task_id="end")

    start >> t1 >> end
