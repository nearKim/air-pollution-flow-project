from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@task()
def test(*args, **kwargs):
    print("test", args, kwargs, flush=True)
    return [1, 2, 3, 4, 5]


@task()
def test2(lst, *args, **kwargs):
    print("test2", args, kwargs, flush=True)
    print(lst)
    print("DONE")


with DAG(
    "task-tutorial",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 11, 1),
    catchup=False,
    tags=["example"],
) as dag:
    result = test()
    test2(result)
    test2(result)
    test2(result)
