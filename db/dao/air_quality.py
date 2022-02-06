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

__all__ = [
    "air_quality",
    "AirQualityORM",
    "AirQualityMeasureCenterORM",
]

from sqlalchemy.ext.declarative import declarative_base

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


class AirQualityORM(Base):
    __tablename__ = "air_quality"

    id = Column("id", BigInteger, primary_key=True)
    measure_datetime = Column("measure_datetime", TIMESTAMP)
    location = Column("location", String(255))
    no2 = Column("no2", Float)
    o3 = Column("o3", Float)
    co = Column("co", Float)
    so2 = Column("so2", Float)
    pm10 = Column("pm10", Float)
    pm25 = Column("pm25", Float)
    upd_ts = Column("upd_ts", TIMESTAMP, server_onupdate=FetchedValue())
    reg_ts = Column("reg_ts", TIMESTAMP, server_default=FetchedValue())


class AirQualityMeasureCenterORM(Base):
    __tablename__ = "air_quality_measure_center"

    id = Column("id", BigInteger, primary_key=True)
    address = Column("address", String(255))
    location = Column("location", String(255))
    official_code = Column("official_code", Integer, unique=True)
    upd_ts = Column("upd_ts", TIMESTAMP, server_onupdate=FetchedValue())
    reg_ts = Column("reg_ts", TIMESTAMP, server_default=FetchedValue())
    coordinate = Column("coordinate", MysqlGeometry("POINT"))

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
