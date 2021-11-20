import typing

import requests

from dto import AirQualityDTO


def request_data(url) -> typing.List[AirQualityDTO]:
    r = requests.get(url)
    assert r.status_code == 200
    j = r.json()
    data_list: typing.List[dict] = j['TimeAverageAirQuality']['row']
    return [AirQualityDTO(**d) for d in data_list]


