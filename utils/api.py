import typing

import requests

from dto import AirQuality


def request_data(url) -> typing.List[AirQuality]:
    r = requests.get(url)
    assert r.status_code == 200
    j = r.json()
    data_list: typing.List[dict] = j['TimeAverageAirQuality']['row']
    return [AirQuality(**d) for d in data_list]


