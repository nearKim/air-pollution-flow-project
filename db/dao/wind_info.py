from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Column,
    Enum,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base

from db.dto import QualityEnum
from infra.sqlalchemy import MysqlGeometry

Base = declarative_base()

metadata = MetaData()

wind_info_measure_center = Table(
    "wind_info_measure_center",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("address", String(255), unique=True),
    Column("location", String(255), unique=True),
    Column("official_code", Integer, unique=True),
    Column("height", Float),
    Column("coordinate", MysqlGeometry("POINT")),
)

wind_info = Table(
    "wind_info",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("measure_datetime", TIMESTAMP),
    Column("station_id", Integer),
    Column("station_name", String(10)),
    Column("temperature", Float, nullable=True),
    Column("is_temperature_normal", Enum(QualityEnum)),
    Column("precipitation", Float, nullable=True),
    Column("is_precipitation_normal", Enum(QualityEnum)),
    Column("wind_speed", Float, nullable=True),
    Column("is_wind_speed_normal", Enum(QualityEnum)),
    Column("wind_direction", Integer, nullable=True),
    Column("is_wind_direction_normal", Enum(QualityEnum)),
    Column("humidity", Integer, nullable=True),
    Column("is_humidity_normal", Enum(QualityEnum)),
    Column("vapor_pressure", Float, nullable=True),
    Column("due_temperature", Float, nullable=True),
    Column("atmosphere_pressure", Float, nullable=True),
    Column("is_atmosphere_pressure_normal", Enum(QualityEnum)),
    Column("sea_level_pressure", Float, nullable=True),
    Column("is_sea_level_pressure_normal", Enum(QualityEnum)),
    Column("sunshine", Float, nullable=True),
    Column("is_sunshine_normal", Enum(QualityEnum)),
    Column("solar_radiation", Float, nullable=True),
    Column("snow_depth", Float, nullable=True),
    Column("cloudiness", Integer, nullable=True),
    Column("low_cloudiness", Integer, nullable=True),
    Column("cloud_formation", String(2), nullable=True),
    Column("least_cloud_height", Integer, nullable=True),
    Column("ground_status", Integer, nullable=True),
    Column("ground_temperature", Float, nullable=True),
    Column("is_ground_temperature_normal", Enum(QualityEnum)),
    Column("ground_5_temperature", Float, nullable=True),
    Column("ground_10_temperature", Float, nullable=True),
    Column("ground_20_temperature", Float, nullable=True),
    Column("ground_30_temperature", Float, nullable=True),
    Column("visibility", Integer, nullable=True),
    UniqueConstraint("measure_datetime", "station_id"),
)


class WindInfoORM(Base):
    _BOOLEAN_COLUMN_NAMES = [
        "is_temperature_normal",
        "is_precipitation_normal",
        "is_wind_speed_normal",
        "is_wind_direction_normal",
        "is_humidity_normal",
        "is_atmosphere_pressure_normal",
        "is_sea_level_pressure_normal",
        "is_sunshine_normal",
        "is_ground_temperature_normal",
    ]

    __tablename__ = "wind_info"

    id = Column("id", BigInteger, primary_key=True)
    measure_datetime = Column("measure_datetime", TIMESTAMP)
    station_id = Column("station_id", Integer)
    station_name = Column("station_name", String(10))
    temperature = Column("temperature", Float, nullable=True)
    is_temperature_normal = Column("is_temperature_normal", Enum(QualityEnum))
    precipitation = Column("precipitation", Float, nullable=True)
    is_precipitation_normal = Column("is_precipitation_normal", Enum(QualityEnum))
    wind_speed = Column("wind_speed", Float, nullable=True)
    is_wind_speed_normal = Column("is_wind_speed_normal", Enum(QualityEnum))
    wind_direction = Column("wind_direction", Integer, nullable=True)
    is_wind_direction_normal = Column("is_wind_direction_normal", Enum(QualityEnum))
    humidity = Column("humidity", Integer, nullable=True)
    is_humidity_normal = Column("is_humidity_normal", Enum(QualityEnum))
    vapor_pressure = Column("vapor_pressure", Float, nullable=True)
    due_temperature = Column("due_temperature", Float, nullable=True)
    atmosphere_pressure = Column("atmosphere_pressure", Float, nullable=True)
    is_atmosphere_pressure_normal = Column(
        "is_atmosphere_pressure_normal", Enum(QualityEnum)
    )
    sea_level_pressure = Column("sea_level_pressure", Float, nullable=True)
    is_sea_level_pressure_normal = Column(
        "is_sea_level_pressure_normal", Enum(QualityEnum)
    )
    sunshine = Column("sunshine", Float, nullable=True)
    is_sunshine_normal = Column("is_sunshine_normal", Enum(QualityEnum))
    solar_radiation = Column("solar_radiation", Float, nullable=True)
    snow_depth = Column("snow_depth", Float, nullable=True)
    cloudiness = Column("cloudiness", Integer, nullable=True)
    low_cloudiness = Column("low_cloudiness", Integer, nullable=True)
    cloud_formation = Column("cloud_formation", String(2), nullable=True)
    least_cloud_height = Column("least_cloud_height", Integer, nullable=True)
    ground_status = Column("ground_status", Integer, nullable=True)
    ground_temperature = Column("ground_temperature", Float, nullable=True)
    is_ground_temperature_normal = Column(
        "is_ground_temperature_normal", Enum(QualityEnum)
    )
    ground_5_temperature = Column("ground_5_temperature", Float, nullable=True)
    ground_10_temperature = Column("ground_10_temperature", Float, nullable=True)
    ground_20_temperature = Column("ground_20_temperature", Float, nullable=True)
    ground_30_temperature = Column("ground_30_temperature", Float, nullable=True)
    visibility = Column("visibility", Integer, nullable=True)

    def serialize(self) -> dict:
        d = self.__dict__
        for k, v in d.items():
            if k in self._BOOLEAN_COLUMN_NAMES:
                if v == QualityEnum.정상:
                    d[k] = True
                elif v == QualityEnum.오류:
                    d[k] = False
                elif v == QualityEnum.결측:
                    d[k] = None
                else:
                    raise ValueError(f"Unknown value: {v}")
        return d


class WindInfoMeasureCenterORM(Base):
    __tablename__ = "wind_info_measure_center"

    id = Column("id", BigInteger, primary_key=True)
    address = Column("address", String(255), unique=True)
    location = Column("location", String(255), unique=True)
    official_code = Column("official_code", Integer, unique=True)
    height = Column("height", Float)
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
