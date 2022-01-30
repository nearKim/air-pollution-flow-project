import typing
from datetime import datetime, timedelta
from time import sleep

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert

from constants import KST
from db.dao.wind_info import wind_info, wind_info_measure_center
from db.dto import WindInfoDTO
from infra.db import engine
from services.wind_info import wind_info_service
from utils.common import convert_to_kst_datetime
from utils.sentry import init_sentry


@task()
def get_measure_center_id_list() -> typing.List[int]:
    with engine.connect() as conn:
        stmt = select([wind_info_measure_center.c.official_code])
        result = conn.execute(stmt)
    return [r[0] for r in result]


@task(task_concurrency=1)
def insert_data_to_db(center_id_list: typing.List[int], **context) -> None:
    dtz = convert_to_kst_datetime(context["ds"], "%Y-%m-%d")
    dto_list: typing.List[WindInfoDTO] = []

    for center_id in center_id_list:
        center_dto_list = wind_info_service.get_wind_info_list(
            target_datetime=dtz, station_id=center_id
        )
        dto_list.extend(center_dto_list)
        sleep(0.1)

    dict_list = wind_info_service.convert_dto_list_to_dict_list(dto_list)

    _stmt = insert(wind_info)
    stmt = _stmt.on_duplicate_key_update(
        id=_stmt.inserted.id,
        measure_datetime=_stmt.inserted.measure_datetime,
        station_id=_stmt.inserted.station_id,
        station_name=_stmt.inserted.station_name,
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
    "insert_wind_info_to_db",
    default_args=default_args,
    description="기상청 ASOS API의 시간자료를 DB에 업데이트합니다.",
    schedule_interval="@daily",
    start_date=KST.convert(datetime(2018, 1, 1)),
    catchup=True,
    max_active_runs=10,
    tags=["wind_info", "DB"],
) as dag:
    init_sentry()

    start = DummyOperator(task_id="start")

    with TaskGroup("update_db") as tg:
        measure_center_id_list = get_measure_center_id_list()
        dumb_result = insert_data_to_db(measure_center_id_list)

    end = DummyOperator(task_id="end")

    start >> tg >> end
