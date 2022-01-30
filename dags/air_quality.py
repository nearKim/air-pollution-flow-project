import typing
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Column,
    FetchedValue,
    Float,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects.mysql import insert

from constants import KST
from db.dto import AirQualityDTO
from infra.db import engine
from services.air_quality import air_quality_service
from utils.common import convert_to_kst_datetime
from utils.sentry import capture_exception_to_sentry, init_sentry

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


@capture_exception_to_sentry
def get_api_result_count(datetime_str: str, **context) -> int:
    dtz = convert_to_kst_datetime(datetime_str, "%Y-%m-%d")
    cnt = air_quality_service.get_result_count(dtz)
    return cnt


@capture_exception_to_sentry
def insert_data_to_db(datetime_str: str, **context) -> typing.NoReturn:
    dtz = convert_to_kst_datetime(datetime_str, "%Y-%m-%d")
    cnt = context["task_instance"].xcom_pull(task_ids="get_api_result_count")
    dto_list: typing.List[AirQualityDTO] = air_quality_service.get_air_quality_list(
        dtz, 1, cnt
    )
    dict_list = air_quality_service.convert_dto_list_to_dict_list(dto_list)

    _stmt = insert(air_quality)
    stmt = _stmt.on_duplicate_key_update(
        id=_stmt.inserted.id,
        measure_datetime=_stmt.inserted.measure_datetime,
        location=_stmt.inserted.location,
        no2=_stmt.inserted.no2,
        o3=_stmt.inserted.o3,
        co=_stmt.inserted.co,
        so2=_stmt.inserted.so2,
        pm10=_stmt.inserted.pm10,
        pm25=_stmt.inserted.pm25,
    )

    with engine.connect() as conn:
        conn.execute(stmt, dict_list)


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
    start_date=datetime(2018, 1, 1, tzinfo=KST),
    catchup=True,
    max_active_runs=5,
    tags=["air_quality", "DB"],
) as dag:
    init_sentry()

    # TODO: Check if the DAG is gathering the air quality of the yesterday
    t1 = PythonOperator(
        task_id="get_api_result_count",
        python_callable=get_api_result_count,
        op_kwargs={"datetime_str": "{{ ds }}"},
    )

    t2 = PythonOperator(
        task_id="insert_data_to_db",
        python_callable=insert_data_to_db,
        op_kwargs={"datetime_str": "{{ ds }}"},
        retry_delay=timedelta(days=1),
    )

    t1 >> t2
