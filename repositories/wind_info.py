import datetime
import typing
from datetime import timedelta

from db.dao.wind_info import WindInfoMeasureCenterORM, WindInfoORM
from infra.db import session


class WindInfoRepository:
    def get_measure_center_by_id(self, id: int) -> WindInfoMeasureCenterORM:
        result = session.query(WindInfoMeasureCenterORM).filter_by(id=id).first()
        return result

    def get_by_measure_date(
        self, measure_date: datetime.date
    ) -> typing.List[WindInfoORM]:
        today, tomorrow = measure_date, measure_date + timedelta(days=1)
        lst = (
            session.query(WindInfoORM)
            .filter(
                WindInfoORM.measure_datetime >= today,
                WindInfoORM.measure_datetime < tomorrow,
            )
            .all()
        )
        return lst


wind_info_repository = WindInfoRepository()
