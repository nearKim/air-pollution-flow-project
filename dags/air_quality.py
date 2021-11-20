from datetime import datetime, timedelta

import typing

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import (
    Table,
    Float,
    TIMESTAMP,
    Column,
    String,
    BigInteger,
    MetaData,
    UniqueConstraint,
    FetchedValue,
)
from sqlalchemy.dialects.mysql import insert

from dto import AirQualityDTO
from infra.db import engine
from services.api import api_service

metadata = MetaData()

air_quality = Table(
    "air_quality",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("measure_datetime", TIMESTAMP),
    Column("location", String(255)),
    Column("no2", Float),
    Column("o3", Float),
    Column("co", Float),
    Column("so2", Float),
    Column("pm10", Float),
    Column("pm25", Float),
    Column("upd_ts", TIMESTAMP, server_onupdate=FetchedValue()),
    Column("reg_ts", TIMESTAMP, server_default=FetchedValue()),
    UniqueConstraint("measure_datetime", "location"),
)


def get_api_result_count(dt: datetime, **context) -> int:
    cnt = api_service.get_result_count(dt)
    return cnt


def insert_data_to_db(dt: datetime, cnt: int, **context) -> typing.NoReturn:
    dto_list: typing.List[AirQualityDTO] = api_service.get_air_quality_list(dt, 1, cnt)
    dict_list = api_service.convert_dto_list_to_dict_list(dto_list)

    stmt_list = []
    for d in dict_list:
        stmt = insert(air_quality).values(**d)
        stmt = stmt.on_duplicate_key_update(
            id=stmt.inserted.id,
            measure_datetime=stmt.inserted.measure_datetime,
            location=stmt.inserted.location,
            no2=stmt.inserted.no2,
            o3=stmt.inserted.o3,
            co=stmt.inserted.co,
            so2=stmt.inserted.so2,
            pm10=stmt.inserted.pm10,
            pm25=stmt.inserted.pm25,
        )
        stmt_list.append(stmt)

    with engine.connect() as conn:
        for stmt in stmt_list:
            conn.execute(stmt)


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
    # TODO: Check if the DAG is gathering the air quality of the yesterday
    t1 = PythonOperator(
        task_id="get_api_result_count",
        python_callable=get_api_result_count,
        op_kwargs={"dt": "{{ ds }}"},
    )

    result_count: int = t1.xcom_pull(task_ids="get_api_result_count")

    t2 = PythonOperator(
        task_id="insert_data_to_db",
        python_callable=insert_data_to_db,
        op_kwargs={"dt": "{{ ds }}", "cnt": result_count},
    )

    t1 >> t2
