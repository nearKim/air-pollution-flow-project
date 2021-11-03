import datetime
from dataclasses import Field

from pydantic import BaseModel, validator


class AirQuality(BaseModel):
    measure_datetime: datetime.datetime = Field(..., alias="MSRDT")
    location: str = Field(..., alias="MSRSTE_NM")
    no2: float
    o3: float
    co: float
    so2: float
    pm10: int
    pm25: int

    @validator("measure_datetime")
    def string_to_datetime(cls, value: str) -> datetime.datetime:
        value = value.rstrip("00")
        return datetime.datetime.strptime(value, "%Y%m%d%H")

    class Config:
        @classmethod
        def alias_generator(cls, string: str) -> str:
            # alias가 없는 변수들을 대문자를 사용하여 instantiate 할 수 있게 한다
            return string.upper()
