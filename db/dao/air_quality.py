import typing
from datetime import datetime

from pydantic.dataclasses import dataclass
from sqlalchemy import (TIMESTAMP, BigInteger, Column, FetchedValue, Float,
                        Integer, MetaData, String, Table, UniqueConstraint)

__all__ = [
    "air_quality",
    "AirQualityORM",
    "AirQualityMeasureCenterORM",
]

from sqlalchemy.orm import declarative_base, mapper

from infra.sqlalchemy import MysqlGeometry

Base = declarative_base()

metadata = MetaData()

air_quality_measure_center = Table(
    "air_quality_measure_center",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("address", String(255)),
    Column("location", String(255)),
    Column("official_code", Integer, unique=True),
    Column("upd_ts", TIMESTAMP, server_onupdate=FetchedValue()),
    Column("reg_ts", TIMESTAMP, server_default=FetchedValue()),
    Column("coordinate", MysqlGeometry("POINT")),
)

air_quality = Table(
    "air_quality",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("measure_datetime", TIMESTAMP),
    Column("location", String(255)),
    Column("no2", Float),
    Column("o3", Float),
    Column("co", Float),
    Column("so2", Float),
    Column("pm10", Float),
    Column("pm25", Float),
    Column("upd_ts", TIMESTAMP, server_onupdate=FetchedValue()),
    Column("reg_ts", TIMESTAMP, server_default=FetchedValue()),
    UniqueConstraint("measure_datetime", "location"),
)


@dataclass
class AirQualityORM:
    __table__ = air_quality

    id: int
    measure_datetime: datetime
    location: str
    no2: float
    o3: float
    co: float
    so2: float
    pm10: float
    pm25: float
    upd_ts: datetime
    reg_ts: datetime


@dataclass
class AirQualityMeasureCenterORM:
    __table__ = air_quality_measure_center
    id: int
    address: str
    location: str
    official_code: int
    upd_ts: datetime
    reg_ts: datetime
    coordinate: typing.Any  # type: WKBElement

    @property
    def shape(self):
        from geoalchemy2.shape import to_shape

        return to_shape(self.coordinate)

    @property
    def latitude(self) -> float:
        return self.shape.x

    @property
    def longitude(self) -> float:
        return self.shape.y


mapper(AirQualityORM, air_quality)
mapper(AirQualityMeasureCenterORM, air_quality_measure_center)
