import appdirs
import os

PINS_NAME = "pins-py"


def get_data_dir():
    return os.environ.get("PINS_DATA_DIR", appdirs.user_data_dir(PINS_NAME))


def get_cache_dir():
    return os.environ.get("PINS_CACHE_DIR", appdirs.user_cache_dir(PINS_NAME))
