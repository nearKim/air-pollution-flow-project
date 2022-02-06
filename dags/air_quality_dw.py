import pathlib
import shutil
import typing
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from constants import KST, TMP_DIR
from db.dto.air_quality import AirQualityWithMeasureCenterInfoDTO
from repositories.air_quality import air_quality_repository
from services.air_quality import air_quality_service
from utils.aws import upload_file
from utils.common import convert_to_kst_datetime, save_json_string_to_parquet
from utils.sentry import init_sentry

BUCKET_NAME = "air-pollution-project-data"


def create_file_dir_path(datetime_str: str, **context):
    pathlib.Path(TMP_DIR / datetime_str).mkdir(parents=True, exist_ok=True)


def delete_file_dir_path(datetime_str: str, **context):
    p = pathlib.Path(TMP_DIR / datetime_str)
    shutil.rmtree(p)


def save_db_data_to_parquet_file(datetime_str: str, **context):
    today = convert_to_kst_datetime(datetime_str, "%Y-%m-%d")
    air_quality_orm_list = air_quality_service.get_measured_air_quality_list(
        today, air_quality_repository
    )
    measure_center_list = air_quality_service.list_air_quality_measure_center_list(
        air_quality_repository
    )
    dto_list: typing.List[
        AirQualityWithMeasureCenterInfoDTO
    ] = air_quality_service.get_air_quality_with_measure_center_info(
        air_quality_orm_list, measure_center_list
    )
    json_str: str = air_quality_service.serialize_to_json(dto_list)
    save_json_string_to_parquet(datetime_str, json_str)


def insert_to_s3(datetime_str: str, **context):
    today = convert_to_kst_datetime(datetime_str, "%Y-%m-%d").date()

    upload_file(
        f"{TMP_DIR}/{datetime_str}/data.parquet",
        BUCKET_NAME,
        f"air_quality__{str(today)}.parquet",
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
    dag_id="air_quality_dw",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2018, 1, 1, tzinfo=KST),
    catchup=True,
    max_active_runs=10,
    tags=["air_quality", "DW"],
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
