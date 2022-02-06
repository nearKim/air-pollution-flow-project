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

from db.dto import QualityEnum
from infra.sqlalchemy import MysqlGeometry

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
