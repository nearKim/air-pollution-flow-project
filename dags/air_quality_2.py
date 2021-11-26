from datetime import datetime, timedelta

import typing
from time import sleep

from airflow import DAG
from airflow.operators.python import PythonOperator


def sleep_short() -> typing.NoReturn:
    sleep(3)


def sleep_well() -> typing.NoReturn:
    sleep(10)


default_args = {
    "owner": "airflow",
    "email": ["garfield@snu.ac.kr", "mschoi523@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "tmp__concurrency_test",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2021, 11, 1),
    catchup=True,
    max_active_runs=5,
    concurrency=2,
    tags=["tmp"],
) as dag:

    t1 = PythonOperator(
        task_id="sleep_short",
        python_callable=sleep_short,
    )

    t2 = PythonOperator(
        task_id="sleep_well",
        python_callable=sleep_well,
        task_concurrency=1,
    )

    t1 >> t2
