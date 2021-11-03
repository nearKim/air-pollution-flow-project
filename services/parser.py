import json
import typing
from functools import lru_cache

import requests

from dto import AirQuality
from utils.api import request_data


class ParserService:
    API_ROOT = "http://openAPI.seoul.go.kr:8088"

    def __init__(self, api_key):
        self.api_key = api_key

    @lru_cache(maxsize=None)
    def result_count(self, target_date_str: str):
        base_url = self.get_api_url(target_date_str, 1, 1)
        r = requests.get(base_url)
        assert r.status_code == 200

        raw_data = json.loads(r.text)
        total_count = raw_data["TimeAverageAirQuality"]["list_total_count"]
        return total_count

    def get_api_url(self, target_date_str: str, start_idx: int, end_idx: int) -> str:
        url = f"{self.API_ROOT}/{self.api_key}/json/TimeAverageAirQuality/{start_idx}/{end_idx}/{target_date_str}"
        return url

    def get_fullday_api_url(self, target_date_str: str) -> str:
        result_count = self.result_count(target_date_str)
        url = self.get_api_url(target_date_str, 1, result_count)
        return url

    def get_air_quality_list(
        self, target_date_str: str, start_idx: int, end_idx: int
    ) -> typing.List[AirQuality]:
        url = self.get_api_url(target_date_str, start_idx, end_idx)
        return request_data(url)

    def get_fullday_air_quality_list(
        self, target_date_str: str
    ) -> typing.List[AirQuality]:
        url = self.get_fullday_api_url(target_date_str)
        return request_data(url)
