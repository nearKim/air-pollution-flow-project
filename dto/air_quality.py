import datetime
from pydantic import BaseModel, validator, Field


class AirQualityDTO(BaseModel):
    measure_datetime_str: str = Field(..., alias="MSRDT")
    location: str = Field(..., alias="MSRSTE_NM")
    no2: float
    o3: float
    co: float
    so2: float
    pm10: float
    pm25: float

    @property
    def measure_datetime(self) -> datetime.datetime:
        return datetime.datetime.strptime(self.measure_datetime_str, "%Y%m%d%H")

    @validator("measure_datetime")
    def strip_double_zeros(cls, value: str):
        value = value.rstrip("00")
        return value

    class Config:
        @classmethod
        def alias_generator(cls, string: str) -> str:
            # alias가 없는 변수들을 대문자를 사용하여 instantiate 할 수 있게 한다
            return string.upper()
