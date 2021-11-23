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
