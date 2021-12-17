import typing
from datetime import datetime

import requests
from dto.wind import WindInfoDTO
from infra.secret import get_secret_data

from utils.common import convert_empty_string_value_to_null

API_KEY = get_secret_data("air-pollution/api")["asos_api_key"]
API_ROOT = (
    f"http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList?"
    f"serviceKey={API_KEY}&dataType=JSON&dataCd=ASOS&dateCd=HR&pageNo=1&numOfRows=999"
)


class WindInfoService:
    DATE_FORMAT = "%Y%m%d"

    def build_one_day_query_string(self, target_datetime: datetime):
        start_dt_str = end_dt_str = target_datetime.strftime(self.DATE_FORMAT)
        start_hh = "00"
        end_hh = "23"
        return f"startDt={start_dt_str}&startHh={start_hh}&endDt={end_dt_str}&endHh={end_hh}"

    def get_api_url(self, target_datetime: datetime, station_id: int) -> str:
        one_day_query_string = self.build_one_day_query_string(target_datetime)
        url = f"{API_ROOT}&{one_day_query_string}&stnIds={station_id}"
        return url

    def request_data(self, url) -> typing.List[WindInfoDTO]:
        r = requests.get(url)
        assert r.status_code == 200
        j = r.json()
        data_list: typing.List[dict] = j["response"]["body"]["items"]["item"]
        for data in data_list:
            convert_empty_string_value_to_null(data)
        return [WindInfoDTO(**d) for d in data_list]

    def get_wind_info_list(
        self, target_datetime: datetime, *, station_id: int
    ) -> typing.List[WindInfoDTO]:
        url = self.get_api_url(target_datetime, station_id)
        return self.request_data(url)


wind_info_service = WindInfoService()

