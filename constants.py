import pathlib

import pendulum

_BASE_DIR = pathlib.Path().resolve()

KST = pendulum.timezone("Asia/Seoul")
TMP_DIR = _BASE_DIR / "tmp"
