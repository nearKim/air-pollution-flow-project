from datetime import datetime

from constants import KST


def convert_empty_string_value_to_null(d: dict) -> None:
    for k, v in d.items():
        if v == "":
            d[k] = None


def convert_to_kst_datetime(datetime_str: str, dt_format: str) -> datetime:
    naive_dt: datetime = datetime.strptime(datetime_str, dt_format)
    return KST.convert(naive_dt)
