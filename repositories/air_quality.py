import datetime
import typing
from datetime import timedelta

from db.dao.air_quality import AirQualityMeasureCenterORM, AirQualityORM
from infra.db import session


class AirQualityRepository:
    def get_by_measure_date(
        self, measure_date: datetime.date
    ) -> typing.List[AirQualityORM]:
        today, tomorrow = measure_date, measure_date + timedelta(days=1)
        lst = (
            session.query(AirQualityORM)
            .filter(
                AirQualityORM.measure_datetime >= today,
                AirQualityORM.measure_datetime < tomorrow,
            )
            .all()
        )
        return lst

    def list_measure_center(self) -> typing.List[AirQualityMeasureCenterORM]:
        result = session.query(AirQualityMeasureCenterORM).all()
        return result


air_quality_repository = AirQualityRepository()
