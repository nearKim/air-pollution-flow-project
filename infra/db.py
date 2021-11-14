from sqlalchemy import create_engine

from infra.secret import get_secret_data


def get_db():
    secret_data = get_secret_data("air-pollution/db")
    username = secret_data["username"]
    password = secret_data["password"]
    host = secret_data["host"]
    engine_uri = f"mysql://{username}:{password}@{host}/air_pollution"
    return create_engine(engine_uri)


engine = get_db()
