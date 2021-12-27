import json
import typing
from datetime import datetime
from functools import lru_cache

import requests
from functional import seq

from dto import AirQualityDTO
from infra.secret import get_secret_data
from utils.common import convert_to_kst_datetime

API_KEY = get_secret_data("air-pollution/api")["open_api_key"]
API_ROOT = f"http://openAPI.seoul.go.kr:8088/{API_KEY}/json/TimeAverageAirQuality"


class AirQualityService:
    DATE_FORMAT = "%Y%m%d"

    @lru_cache(maxsize=None)
    def get_result_count(self, target_datetime: datetime):
        target_datetime_str = target_datetime.strftime(self.DATE_FORMAT)
        base_url = self.get_api_url(target_datetime_str, 1, 1)
        r = requests.get(base_url)

        raw_data = json.loads(r.text)
        total_count = raw_data["TimeAverageAirQuality"]["list_total_count"]
        return total_count

    def get_api_url(self, target_date_str: str, start_idx: int, end_idx: int) -> str:
        url = f"{API_ROOT}/{start_idx}/{end_idx}/{target_date_str}"
        return url

    def request_data(self, url) -> typing.List[AirQualityDTO]:
        r = requests.get(url)
        assert r.status_code == 200
        j = r.json()
        data_list: typing.List[dict] = j["TimeAverageAirQuality"]["row"]
        return [AirQualityDTO(**d) for d in data_list]

    def get_air_quality_list(
        self, target_datetime: datetime, start_idx: int, end_idx: int
    ) -> typing.List[AirQualityDTO]:
        target_datetime_str = target_datetime.strftime(self.DATE_FORMAT)
        url = self.get_api_url(target_datetime_str, start_idx, end_idx)
        return self.request_data(url)

    def convert_dto_list_to_dict_list(
        self, dto_list: typing.List[AirQualityDTO]
    ) -> typing.List[typing.Dict]:
        def add_datetime_to_dict(d: dict):
            measure_datetime_str = d.pop("measure_datetime_str")
            d["measure_datetime"] = convert_to_kst_datetime(
                measure_datetime_str, "%Y%m%d%H"
            )
            return d

        dict_list = (
            seq(dto_list).map(lambda d: d.dict()).map(add_datetime_to_dict).to_list()
        )

        return dict_list


air_quality_service = AirQualityService()
