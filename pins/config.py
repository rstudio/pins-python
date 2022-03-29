import appdirs
import os


def get_data_dir():
    return os.environ.get("PINS_DATA_DIR", appdirs.user_data_dir("pins"))


def get_cache_dir():
    return os.environ.get("PINS_CACHE_DIR", appdirs.user_cache_dir("pins"))
