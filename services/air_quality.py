import json
import typing
from dataclasses import asdict
from datetime import datetime
from functools import lru_cache

import requests
from functional import seq

from db.dao.air_quality import AirQualityMeasureCenterORM, AirQualityORM
from db.dto import AirQualityDTO
from db.dto.air_quality import AirQualityWithMeasureCenterInfoDTO
from infra.secret import get_secret_data
from repositories.air_quality import AirQualityRepository
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

    def get_measured_air_quality_list(
        self, target_datetime: datetime, repository: AirQualityRepository
    ) -> typing.List[AirQualityORM]:
        air_quality_orm_list: typing.List[
            AirQualityORM
        ] = repository.get_by_measure_date(target_datetime.date())
        return air_quality_orm_list

    def list_air_quality_measure_center_list(
        self, repository: AirQualityRepository
    ) -> typing.List[AirQualityMeasureCenterORM]:
        orm_list: typing.List[
            AirQualityMeasureCenterORM
        ] = repository.list_measure_center()
        return orm_list

    def get_air_quality_with_measure_center_info(
        self, air_quality_orm_list, measure_center_orm_list
    ) -> typing.List[AirQualityWithMeasureCenterInfoDTO]:
        result = []
        for air_quality in air_quality_orm_list:
            air_quality: AirQualityORM
            center = [
                c for c in measure_center_orm_list if c.location == air_quality.location
            ]
            assert len(center) == 1
            center = center[0]

            dto = AirQualityWithMeasureCenterInfoDTO(
                **asdict(air_quality),
                measure_center_address=center.address,
                measure_center_official_code=center.official_code,
                measure_center_latitude=center.latitude,
                measure_center_longitude=center.longitude,
            )
            result.append(dto)
        return result

    def serialize_to_json(
        self, dto_list: typing.List[AirQualityWithMeasureCenterInfoDTO]
    ) -> str:
        def serialize_to_json(j) -> str:
            return json.dumps(j, default=str, ensure_ascii=False)

        dto_list = (
            seq(dto_list).map(lambda orm: asdict(orm)).map(serialize_to_json).list()
        )
        return "\n".join(dto_list)


air_quality_service = AirQualityService()
