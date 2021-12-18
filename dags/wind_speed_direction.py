import typing
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from geoalchemy2 import Geometry
from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Column,
    Enum,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    select,
)

from dto.wind import QualityEnum, WindInfoDTO
from infra.db import engine
from services.wind_info import wind_info_service
from utils.sentry import capture_exception_to_sentry, init_sentry

metadata = MetaData()

wind_info_measure_center = Table(
    "wind_info_measure_center",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("address", String(255), unique=True),
    Column("location", String(255), unique=True),
    Column("official_code", Integer, unique=True),
    Column("height", Float),
    Column("coordinate", Geometry("POINT")),
)

wind_info = Table(
    "wind_info",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("measure_datetime", TIMESTAMP),
    Column("station_id", Integer),
    Column("station_name", String(10)),
    Column("temperature", Float, nullable=True),
    Column("is_temperature_normal", Enum(QualityEnum)),
    Column("precipitation", Float, nullable=True),
    Column("is_precipitation_normal", Enum(QualityEnum)),
    Column("wind_speed", Float, nullable=True),
    Column("is_wind_speed_normal", Enum(QualityEnum)),
    Column("wind_direction", Integer, nullable=True),
    Column("is_wind_direction_normal", Enum(QualityEnum)),
    Column("humidity", Integer, nullable=True),
    Column("is_humidity_normal", Enum(QualityEnum)),
    Column("vapor_pressure", Float, nullable=True),
    Column("due_temperature", Float, nullable=True),
    Column("atmosphere_pressure", Float, nullable=True),
    Column("is_atmosphere_pressure_normal", Enum(QualityEnum)),
    Column("sea_level_pressure", Float, nullable=True),
    Column("is_sea_level_pressure_normal", Enum(QualityEnum)),
    Column("sunshine", Float, nullable=True),
    Column("is_sunshine_normal", Enum(QualityEnum)),
    Column("solar_radiation", Float, nullable=True),
    Column("snow_depth", Float, nullable=True),
    Column("cloudiness", Integer, nullable=True),
    Column("low_cloudiness", Integer, nullable=True),
    Column("cloud_formation", String(2), nullable=True),
    Column("least_cloud_height", Integer, nullable=True),
    Column("ground_status", Integer, nullable=True),
    Column("ground_temperature", Float, nullable=True),
    Column("is_ground_temperature_normal", Enum(QualityEnum)),
    Column("ground_5_temperature", Float, nullable=True),
    Column("ground_10_temperature", Float, nullable=True),
    Column("ground_20_temperature", Float, nullable=True),
    Column("ground_30_temperature", Float, nullable=True),
    Column("visibility", Integer, nullable=True),
    UniqueConstraint("measure_datetime", "station_id"),
)


@task()
def get_measure_center_id_list() -> typing.List[int]:
    with engine.connect() as conn:
        stmt = select([wind_info_measure_center.c.official_code])
        result = conn.execute(stmt)
    return [r[0] for r in result]


@task()
def insert_data_to_db(center_id_list: typing.List[int], **context) -> None:
    datetime_str = context["ds"]
    dt = datetime.strptime(datetime_str, "%Y-%m-%d")
    dto_list: typing.List[WindInfoDTO] = []

    for center_id in center_id_list:
        center_dto_list = wind_info_service.get_wind_info_list(
            target_datetime=dt, station_id=center_id
        )
        dto_list.extend(center_dto_list)

    dict_list = wind_info_service.convert_dto_list_to_dict_list(dto_list)

    with engine.connect() as conn:
        conn.execute(wind_info.insert(), dict_list)


default_args = {
    "owner": "airflow",
    "email": ["garfield@snu.ac.kr", "mschoi523@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "insert_wind_data_to_db",
    default_args=default_args,
    description="기상청 ASOS API의 시간자료를 DB에 업데이트합니다.",
    schedule_interval="@daily",
    start_date=datetime(2021, 11, 1),
    catchup=True,
    max_active_runs=5,
    tags=["wind_info", "DB"],
) as dag:
    init_sentry()

    start = DummyOperator(task_id="start")

    measure_center_id_list = get_measure_center_id_list()
    insert_data_to_db(measure_center_id_list)

    end = DummyOperator(task_id="end")

    start >> measure_center_id_list
