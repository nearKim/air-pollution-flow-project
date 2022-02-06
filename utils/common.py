import typing
from datetime import datetime

from constants import KST, TMP_DIR

if typing.TYPE_CHECKING:
    from pyarrow import Table


def convert_empty_string_value_to_null(d: dict) -> None:
    for k, v in d.items():
        if v == "":
            d[k] = None


def convert_to_kst_datetime(datetime_str: str, dt_format: str) -> datetime:
    naive_dt: datetime = datetime.strptime(datetime_str, dt_format)
    return KST.convert(naive_dt)


def save_json_data_to_file(path: str, data: str):
    with open(f"{path}/data.json", "w+") as f:
        f.write(data)


def save_parquet_data_to_file(path: str, table: "Table"):
    import pyarrow.parquet as pq

    pq.write_table(table, f"{path}/data.parquet")


def save_json_string_to_parquet(datetime_str, json_str: str) -> typing.NoReturn:
    file_dir_path = f"{TMP_DIR}/{datetime_str}"
    save_json_data_to_file(file_dir_path, json_str)
    table = get_pyarrow_table_from_tmp_dir(file_dir_path)
    save_parquet_data_to_file(file_dir_path, table)


def get_pyarrow_table_from_tmp_dir(path: str) -> "Table":
    from pyarrow import json

    table = json.read_json(f"{path}/data.json")
    return table
