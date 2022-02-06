import datetime
import typing
from datetime import timedelta

from sqlalchemy import select

from db.dao.air_quality import AirQualityORM
from infra.db import Session


class AirQualityRepository:
    def get_by_measure_date(
        self, measure_date: datetime.date
    ) -> typing.List[AirQualityORM]:
        today, tomorrow = measure_date, measure_date + timedelta(days=1)
        stmt = select(AirQualityORM).where(
            AirQualityORM.measure_datetime >= today,
            AirQualityORM.measure_datetime < tomorrow,
        )
        with Session() as session:
            list_or_tuples = session.execute(stmt).all()
        result = [tup[0] for tup in list_or_tuples]
        return result


air_quality_repository = AirQualityRepository()
