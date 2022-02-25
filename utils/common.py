import json
import typing
from datetime import datetime

from functional import seq

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


def save_json_data_to_file(filename: str, path: str, data: str):
    with open(f"{path}/{filename}.json", "w+") as f:
        f.write(data)


def save_parquet_data_to_file(path: str, table: "Table"):
    import pyarrow.parquet as pq

    pq.write_table(table, f"{path}/data.parquet")


def save_json_string_to_parquet(
    datetime_str, filename: str, json_str: str
) -> typing.NoReturn:
    file_dir_path = f"{TMP_DIR}/{datetime_str}"
    save_json_data_to_file(filename, file_dir_path, json_str)
    table = get_pyarrow_table_from_tmp_dir(filename, file_dir_path)
    save_parquet_data_to_file(file_dir_path, table)


def get_pyarrow_table_from_tmp_dir(filename, path: str) -> "Table":
    from pyarrow import json

    table = json.read_json(f"{path}/{filename}.json")
    return table


def serialize_to_json(dto_list) -> str:
    def to_json(j) -> str:
        return json.dumps(j, default=str, ensure_ascii=False)

    dto_list = seq(dto_list).map(lambda orm: orm.__dict__).map(to_json).list()
    return "\n".join(dto_list)
