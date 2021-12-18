from datetime import datetime


def convert_empty_string_value_to_null(d: dict) -> None:
    for k, v in d.items():
        if v == "":
            d[k] = None


def add_datetime_to_dict(d: dict, dt_format: str = "%Y%m%d%H"):
    measure_datetime_str = d.pop("measure_datetime_str")
    d["measure_datetime"] = datetime.strptime(measure_datetime_str, dt_format)
    return d
