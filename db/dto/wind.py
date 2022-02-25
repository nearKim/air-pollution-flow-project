from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, validator


class QualityEnum(str, Enum):
    정상 = "NORMAL"
    오류 = "ERROR"
    결측 = "NULL"


class WindInfoWithMeasureCenterInfoDTO(BaseModel):
    measure_datetime_str: str
    station_id: int
    station_name: str
    temperature: Optional[float]
    is_temperature_normal: Optional[bool]
    precipitation: Optional[float]
    is_precipitation_normal: Optional[bool]
    wind_speed: Optional[float]
    is_wind_speed_normal: Optional[bool]
    wind_direction: Optional[int]
    is_wind_direction_normal: Optional[bool]
    humidity: Optional[int]
    is_humidity_normal: Optional[bool]
    vapor_pressure: Optional[float]
    due_temperature: Optional[float]
    atmosphere_pressure: Optional[float]
    is_atmosphere_pressure_normal: Optional[bool]
    sea_level_pressure: Optional[float]
    is_sea_level_pressure_normal: Optional[bool]
    sunshine: Optional[float]
    is_sunshine_normal: Optional[bool]
    solar_radiation: Optional[float]
    snow_depth: Optional[float]
    cloudiness: Optional[int]
    low_cloudiness: Optional[int]
    cloud_formation: Optional[str]
    least_cloud_height: Optional[int]
    ground_status: Optional[int]
    ground_temperature: Optional[float]
    is_ground_temperature_normal: Optional[bool]
    ground_5_temperature: Optional[float]
    ground_10_temperature: Optional[float]
    ground_20_temperature: Optional[float]
    ground_30_temperature: Optional[float]
    visibility: Optional[int]
    measure_center_address: str
    measure_center_official_code: int
    measure_center_latitude: float
    measure_center_longitude: float


class WindInfoDTO(BaseModel):
    measure_datetime_str: str = Field(alias="tm", description="일시")
    station_id: int = Field(alias="stnId", description="종관기상관측 지점 번호")
    station_name: str = Field(alias="stnNm", description="종관기상관측 지점명")
    temperature: Optional[float] = Field(alias="ta", description="기온")
    is_temperature_normal: QualityEnum = Field(
        alias="taQcflg", description="기온 품질검사 플래그"
    )
    precipitation: Optional[float] = Field(alias="rn", description="강수량")
    is_precipitation_normal: QualityEnum = Field(
        alias="rnQcflg", description="강수량 품질검사 플래그"
    )
    wind_speed: Optional[float] = Field(alias="ws", description="풍속")
    is_wind_speed_normal: QualityEnum = Field(
        alias="wsQcflg", description="풍속 품질검사 플래그"
    )
    wind_direction: Optional[int] = Field(alias="wd", description="풍속")
    is_wind_direction_normal: QualityEnum = Field(
        alias="wdQcflg", description="풍향 품질검사 플래그"
    )
    humidity: Optional[int] = Field(alias="hm", description="습도")
    is_humidity_normal: QualityEnum = Field(alias="hmQcflg", description="습도 품질검사 플래그")
    vapor_pressure: Optional[float] = Field(alias="pv", description="증기압")
    due_temperature: Optional[float] = Field(alias="td", description="이슬점온도")
    atmosphere_pressure: Optional[float] = Field(alias="pa", description="현지기압")
    is_atmosphere_pressure_normal: QualityEnum = Field(
        alias="paQcflg", description="현지기압 품질검사 플래그"
    )
    sea_level_pressure: Optional[float] = Field(alias="ps", description="해면기압")
    is_sea_level_pressure_normal: QualityEnum = Field(
        alias="psQcflg", description="해면기압 품질검사 플래그"
    )
    sunshine: Optional[float] = Field(alias="ss", description="일조")
    is_sunshine_normal: QualityEnum = Field(alias="ssQcflg", description="일조 품질검사 플래그")
    solar_radiation: Optional[float] = Field(alias="icsr", description="일사")
    snow_depth: Optional[float] = Field(alias="dsnw", description="적설량")
    cloudiness: Optional[int] = Field(alias="dc10Tca", description="전운량")
    low_cloudiness: Optional[int] = Field(alias="dc10LmcsCa", description="중하층운량")
    cloud_formation: Optional[str] = Field(alias="clfmAbbrCd", description="운형")
    least_cloud_height: Optional[int] = Field(alias="lcsCh", description="최저운고")
    ground_status: Optional[int] = Field(alias="gndSttCd", description="지면상태")
    ground_temperature: Optional[float] = Field(alias="ts", description="지면온도")
    is_ground_temperature_normal: QualityEnum = Field(
        alias="tsQcflg", description="지면온도 품질검사 플래그"
    )
    ground_5_temperature: Optional[float] = Field(
        alias="m005Te", description="5cm 지중온도"
    )
    ground_10_temperature: Optional[float] = Field(
        alias="m01Te", description="10cm 지중온도"
    )
    ground_20_temperature: Optional[float] = Field(
        alias="m02Te", description="20cm 지중온도"
    )
    ground_30_temperature: Optional[float] = Field(
        alias="m03Te", description="30cm 지중온도"
    )
    visibility: Optional[int] = Field("vs", description="시정")

    @validator(
        "is_temperature_normal",
        "is_precipitation_normal",
        "is_wind_speed_normal",
        "is_wind_direction_normal",
        "is_humidity_normal",
        "is_atmosphere_pressure_normal",
        "is_sea_level_pressure_normal",
        "is_sunshine_normal",
        "is_ground_temperature_normal",
        pre=True,
    )
    def convert_to_enum(cls, value: Optional[str]):
        if value:
            value = int(value)

        if not value:
            en = QualityEnum.정상
        elif value == 1:
            en = QualityEnum.오류
        elif value == 9:
            en = QualityEnum.결측
        else:
            raise ValueError(f"QualityEnum으로 파싱할 수 없습니다. {value}")
        return en
