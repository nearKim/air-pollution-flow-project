import typing
from datetime import datetime
from json import JSONDecodeError

import requests
from functional import seq

from db.dao.wind_info import WindInfoMeasureCenterORM, WindInfoORM
from db.dto import WindInfoDTO
from db.dto.wind import WindInfoWithMeasureCenterInfoDTO
from infra.secret import get_secret_data
from repositories.wind_info import WindInfoRepository
from utils.common import convert_empty_string_value_to_null, convert_to_kst_datetime

API_KEY = get_secret_data("air-pollution/api")["asos_api_key"]
API_ROOT = (
    f"http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList?"
    f"serviceKey={API_KEY}&dataType=JSON&dataCd=ASOS&dateCd=HR&pageNo=1&numOfRows=999"
)


class LimitExceededError(Exception):
    pass


class WindInfoService:
    DATE_FORMAT = "%Y%m%d"

    def get_measured_wind_info_list(
        self, target_datetime: datetime, repository: WindInfoRepository
    ):
        wind_info_orm_list: typing.List[WindInfoORM] = repository.get_by_measure_date(
            target_datetime.date()
        )
        return wind_info_orm_list

    def get_wind_info_with_measure_center_info(
        self,
        wind_info_orm_list: typing.List[WindInfoORM],
        wind_info_repository: WindInfoRepository,
    ) -> typing.List[WindInfoWithMeasureCenterInfoDTO]:
        result = []
        for wind_info in wind_info_orm_list:
            center: WindInfoMeasureCenterORM = (
                wind_info_repository.get_measure_center_by_id(wind_info.station_id)
            )

            assert center, f"{wind_info.station_id} is not found"

            dto = WindInfoWithMeasureCenterInfoDTO(
                **wind_info.__dict__,
                measure_datetime_str=wind_info.measure_datetime.strftime(self.DATE_FORMAT),
                measure_center_address=center.address,
                measure_center_official_code=center.official_code,
                measure_center_latitude=center.latitude,
                measure_center_longitude=center.longitude,
            )
            result.append(dto)

        return result

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
        try:
            j = r.json()
        except JSONDecodeError as e:
            response_text = r.text
            if "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR" in response_text:
                raise LimitExceededError from e
            raise e
        try:
            data_list: typing.List[dict] = j["response"]["body"]["items"]["item"]
        except KeyError as e:
            if j["response"]["header"]["resultMsg"] == "NO_DATA":
                return []
            raise e

        for data in data_list:
            convert_empty_string_value_to_null(data)
        return [WindInfoDTO(**d) for d in data_list]

    def get_wind_info_list(
        self, target_datetime: datetime, *, station_id: int
    ) -> typing.List[WindInfoDTO]:
        url = self.get_api_url(target_datetime, station_id)
        return self.request_data(url)

    def convert_dto_list_to_dict_list(
        self, dto_list: typing.List[WindInfoDTO]
    ) -> typing.List[typing.Dict]:
        def add_datetime_to_dict(d: dict):
            measure_datetime_str = d.pop("measure_datetime_str")
            d["measure_datetime"] = convert_to_kst_datetime(
                measure_datetime_str, "%Y-%m-%d %H:00"
            )
            return d

        dict_list = (
            seq(dto_list).map(lambda d: d.dict()).map(add_datetime_to_dict).to_list()
        )

        return dict_list


wind_info_service = WindInfoService()
