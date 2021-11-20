from datetime import datetime, timedelta

import typing

from airflow import DAG

from dto import AirQualityDTO


def get_api_result_count(dt: datetime, **context) -> int:
    pass


def get_api_result_list(
    dt: datetime, result_count: int, **context
) -> typing.List[AirQualityDTO]:
    pass


def insert_data_to_db(dt: datetime, **context) -> typing.NoReturn:
    pass


default_args = {
    "owner": "airflow",
    "email": ["garfield@snu.ac.kr", "mschoi523@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "insert_data_to_db",
    default_args=default_args,
    description="서울시 대기환경 API의 리스폰스를 DB에 업데이트합니다.",
    schedule_interval="@daily",
    start_date=datetime(2021, 11, 1),
    catchup=True,
):
    pass
