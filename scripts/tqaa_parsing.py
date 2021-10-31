import json
import pandas as pd
import requests
from typing import Optional
import yaml

OPEN_API_KEY: str = yaml.load(open("config.yml"), Loader=yaml.FullLoader)[
    "OPEN_API_KEY"
]


def get_query_url(
    date: str = "20211031",
    start_idx: int = 1,
    end_idx: int = 1,
) -> str:
    url = (
        f"http://openAPI.seoul.go.kr:8088/{OPEN_API_KEY}/json/"
        f"TimeAverageAirQuality/{start_idx}/{end_idx}/{date}"
    )
    return url


def get_fullday_query_url(date: str = "20211031") -> str:
    url = get_query_url(date, 1, 1)
    r = requests.get(url)
    assert r.status_code == 200

    raw_data = json.loads(r.text)
    total_count = raw_data["TimeAverageAirQuality"]["list_total_count"]
    url = get_query_url(date, 1, total_count)
    return url


def print_variable_verbose(var, name: Optional[str] = None):
    if name:
        print(f"{name}: ", end="")
    print(var, type(var))


def query(date: str = "20211031", verbose: bool = False) -> pd.DataFrame:
    url = get_fullday_query_url(date=date)
    r = requests.get(url)
    assert r.status_code == 200
    if verbose:
        print_variable_verbose(r.status_code)
        print_variable_verbose(print(r.text))
    raw_data = json.loads(r.text)
    data = raw_data["TimeAverageAirQuality"]
    if verbose:
        print("- type:", type(data))
        print("- keys:", data.keys())
        print("  - list_total_count:", data["list_total_count"])
        print("  - RESULT:", data["RESULT"])
    df = pd.json_normalize(data["row"])
    area_list = sorted(df.MSRSTE_NM.unique().tolist())
    msrdt_list = sorted(df.MSRDT.unique().tolist())
    dates = list({s[:8] for s in msrdt_list})
    hours = [s[8:] for s in msrdt_list]
    if verbose:
        print(f"  - MSRDT dates: ({len(dates)})", dates)
        print(f"  - MSRDT hours: ({len(hours)})", hours)
        print(f"  - MSRSTE_NM: ({len(area_list)})", area_list)
        print("  - row shape:", df.shape)
        print(df)
    return df


if __name__ == "__main__":
    query(verbose=True)
