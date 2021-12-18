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
    """
    context를 inject해줌
    kwargs = {
        "conf": "<airflow.configuration.AirflowConfigParser object at 0x7f955f9bcf50>",
        "dag": "<DAG: task-tutorial>",
        "dag_run": "<DagRun task-tutorial @ 2021-12-17 04:05:42.465357+00:00: scheduled__2021-12-17T04:05:42.465357+00:00, externally triggered: False>",
        "data_interval_end": DateTime(
            2021, 12, 18, 4, 5, 42, 465357, tzinfo=Timezone("UTC")
        ),
        "data_interval_start": DateTime(
            2021, 12, 17, 4, 5, 42, 465357, tzinfo=Timezone("UTC")
        ),
        "ds": "2021-12-17",
        "ds_nodash": "20211217",
        "execution_date": "<Proxy at 0x7f954e0f5190 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e1079e0>>",
        "inlets": [],
        "logical_date": DateTime(
            2021, 12, 17, 4, 5, 42, 465357, tzinfo=Timezone("UTC")
        ),
        "macros": "<module 'airflow.macros' from '/home/airflow/.local/lib/python3.7/site-packages/airflow/macros/__init__.py'>",
        "next_ds": "<Proxy at 0x7f954e0f5140 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e0f7170>>",
        "next_ds_nodash": "<Proxy at 0x7f954e0f5cd0 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e0f7dd0>>",
        "next_execution_date": "<Proxy at 0x7f954e0f5be0 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e0f7e60>>",
        "outlets": [],
        "params": {},
        "prev_data_interval_start_success": "<Proxy at 0x7f954e0f5b40 with factory <function TaskInstance.get_template_context.<locals>.get_prev_data_interval_start_success at 0x7f954f959e60>>",
        "prev_data_interval_end_success": "<Proxy at 0x7f954e0f5f50 with factory <function TaskInstance.get_template_context.<locals>.get_prev_data_interval_end_success at 0x7f954f959c20>>",
        "prev_ds": "<Proxy at 0x7f954e0f5e60 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e0f70e0>>",
        "prev_ds_nodash": "<Proxy at 0x7f954e0f5c30 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e0f7290>>",
        "prev_execution_date": "<Proxy at 0x7f954e0f5910 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e0f7200>>",
        "prev_execution_date_success": "<Proxy at 0x7f95501f1dc0 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e0f7830>>",
        "prev_start_date_success": "<Proxy at 0x7f954e10ff00 with factory <function TaskInstance.get_template_context.<locals>.get_prev_start_date_success at 0x7f954e107f80>>",
        "run_id": "scheduled__2021-12-17T04:05:42.465357+00:00",
        "task": "<Task(_PythonDecoratedOperator): test2__1>",
        "task_instance": "<TaskInstance: task-tutorial.test2__1 scheduled__2021-12-17T04:05:42.465357+00:00 [running]>",
        "task_instance_key_str": "task-tutorial__test2__1__20211217",
        "test_mode": False,
        "ti": "<TaskInstance: task-tutorial.test2__1 scheduled__2021-12-17T04:05:42.465357+00:00 [running]>",
        "tomorrow_ds": "<Proxy at 0x7f954e10f820 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e0f7ef0>>",
        "tomorrow_ds_nodash": "<Proxy at 0x7f954e10ff50 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e0f7f80>>",
        "ts": "2021-12-17T04:05:42.465357+00:00",
        "ts_nodash": "20211217T040542",
        "ts_nodash_with_tz": "20211217T040542.465357+0000",
        "var": {"json": None, "value": None},
        "conn": "<airflow.models.taskinstance.TaskInstance.get_template_context.<locals>.ConnectionAccessor object at 0x7f954e106e90>",
        "yesterday_ds": "<Proxy at 0x7f954e10f9b0 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e0f77a0>>",
        "yesterday_ds_nodash": "<Proxy at 0x7f954e10fcd0 with factory <function TaskInstance.get_template_context.<locals>.deprecated_proxy.<locals>.deprecated_func at 0x7f954e0f7d40>>",
        "templates_dict": None,
    }
    """
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
    print("HERE!!!", result, flush=True)
    test2(result)
    test2(result)
    test2(result)
