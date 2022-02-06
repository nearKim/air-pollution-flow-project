from datetime import datetime

from geoalchemy2 import Geometry
from pydantic.dataclasses import dataclass
from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Column,
    FetchedValue,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)

__all__ = ["air_quality", "AirQualityORM"]

from infra.db import mapper_registry

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
    Column("coordinate", Geometry("POINT")),
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


mapper_registry.map_imperatively(AirQualityORM, air_quality)
