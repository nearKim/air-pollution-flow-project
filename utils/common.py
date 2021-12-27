from datetime import datetime

from constants import KST


def convert_empty_string_value_to_null(d: dict) -> None:
    for k, v in d.items():
        if v == "":
            d[k] = None


def add_datetime_to_dict(d: dict, dt_format: str = "%Y%m%d%H"):
    measure_datetime_str = d.pop("measure_datetime_str")
    d["measure_datetime"] = get_kst_datetime(measure_datetime_str, dt_format)
    return d


def get_kst_datetime(datetime_str: str, dt_format: str):
    dt = datetime.strptime(datetime_str, dt_format)
    dtz = KST.localize(dt)
    return dtz
