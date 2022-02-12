import pathlib
import shutil
import typing
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from constants import KST, TMP_DIR
from db.dao.wind_info import WindInfoORM
from db.dto.wind import WindInfoWithMeasureCenterInfoDTO
from repositories.wind_info import wind_info_repository
from services.wind_info import wind_info_service
from utils.aws import upload_file
from utils.common import (
    convert_to_kst_datetime,
    save_json_string_to_parquet,
    serialize_to_json,
)
from utils.sentry import init_sentry

BUCKET_NAME = "air-pollution-project-data"


def create_file_dir_path(datetime_str, **context):
    pathlib.Path(TMP_DIR / datetime_str).mkdir(parents=True, exist_ok=True)


def delete_file_dir_path(datetime_str: str, **context):
    p = pathlib.Path(TMP_DIR / datetime_str)
    shutil.rmtree(p)


def save_db_data_to_parquet_file(datetime_str: str, **context):
    today = convert_to_kst_datetime(datetime_str, "%Y-%m-%d")
    wind_info_orm_list: typing.List[
        WindInfoORM
    ] = wind_info_service.get_measured_wind_info_list(today, wind_info_repository)

    dto_list: typing.List[
        WindInfoWithMeasureCenterInfoDTO
    ] = wind_info_service.get_wind_info_with_measure_center_info(
        wind_info_orm_list, wind_info_repository
    )
    json_str: str = serialize_to_json(dto_list)
    save_json_string_to_parquet(datetime_str, "wind_info", json_str)


def insert_to_s3(datetime_str: str, **context):
    today = convert_to_kst_datetime(datetime_str, "%Y-%m-%d").date()
    path = f"{BUCKET_NAME}/wind_info/measure_date={str(today)}"
    upload_file(
        f"{TMP_DIR}/{datetime_str}/data.parquet",
        path,
        f"data__{str(today)}.parquet",
    )


default_args = {
    "owner": "airflow",
    "email": ["garfield@snu.ac.kr", "mschoi523@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="wind_speed_dw",
    description="풍향, 풍속 정보를 S3에 저장합니다.",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2018, 1, 1, tzinfo=KST),
    catchup=True,
    max_active_runs=10,
    tags=["wind_info", "DW"],
) as dag:
    init_sentry()

    start = DummyOperator(task_id="start")

    create_tmp_dir = PythonOperator(
        task_id="create_tmp_dir",
        python_callable=create_file_dir_path,
        op_kwargs={"datetime_str": "{{ ds }}"},
    )
    t1 = PythonOperator(
        task_id="save_db_data_to_parquet_file",
        python_callable=save_db_data_to_parquet_file,
        op_kwargs={"datetime_str": "{{ ds }}"},
        task_concurrency=1,  # filename이 겹칠 수 있음
    )

    t2 = PythonOperator(
        task_id="insert_to_s3",
        python_callable=insert_to_s3,
        op_kwargs={"datetime_str": "{{ ds }}"},
    )

    delete_tmp_dir = PythonOperator(
        task_id="delete_tmp_dir",
        python_callable=delete_file_dir_path,
        op_kwargs={"datetime_str": "{{ ds }}"},
    )
    end = DummyOperator(task_id="end")

    start >> create_tmp_dir >> t1 >> t2 >> delete_tmp_dir >> end
