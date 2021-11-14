from datetime import datetime
from typing import Optional

from sqlmodel import SQLModel, Field


class AirQuality(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    measure_datetime: datetime
    location: str  # MySQL: VARCHAR(255)
    no2: float
    o3: float
    co: float
    so2: float
    pm10: int
    pm25: int
    upd_ts: datetime
    reg_ts: datetime

