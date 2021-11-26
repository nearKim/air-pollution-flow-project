from functools import wraps

import sentry_sdk


def capture_exception_to_sentry(func):
    @wraps(func)
    def inner(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            sentry_sdk.capture_exception(e)
            raise e
        return result

    return inner


def init_sentry(debug=False, env="live"):
    from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

    sentry_sdk.init(
        "https://0a708c6f453d4637afcbe446f3427120@o257313.ingest.sentry.io/6068203",
        debug=debug,
        environment=env,
        integrations=[SqlalchemyIntegration()],
        _experiments={
            "record_sql_params": True,
        },
        traces_sample_rate=1.0,
    )
