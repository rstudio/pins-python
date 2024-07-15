import os
from types import SimpleNamespace

import platformdirs

PINS_NAME = "pins-py"
PINS_ENV_DATA_DIR = "PINS_DATA_DIR"
PINS_ENV_CACHE_DIR = "PINS_CACHE_DIR"
PINS_ENV_INSECURE_READ = "PINS_ALLOW_PICKLE_READ"
PINS_ENV_ALLOW_RSC_SHORT_NAME = "PINS_ALLOW_RSC_SHORT_NAME"
PINS_ENV_FEATURE_PREVIEW = "PINS_FEATURE_PREVIEW"

pins_options = SimpleNamespace(quiet=False)


def _interpret_int(env_var_name):
    env_var = os.environ.get(env_var_name, "0")
    try:
        env_int = int(env_var)
    except ValueError:
        raise ValueError(
            f"{env_var_name} must be '0' or '1', but was set to " f"{repr(env_var)}."
        )

    flag = bool(env_int)
    return flag


def get_data_dir():
    return os.environ.get(PINS_ENV_DATA_DIR, platformdirs.user_data_dir(PINS_NAME))


def get_cache_dir():
    return os.environ.get(PINS_ENV_CACHE_DIR, platformdirs.user_cache_dir(PINS_NAME))


def get_allow_pickle_read(flag):
    if flag is None:
        return _interpret_int(PINS_ENV_INSECURE_READ)

    return flag


def get_allow_rsc_short_name():
    return _interpret_int(PINS_ENV_ALLOW_RSC_SHORT_NAME)


def get_feature_preview():
    return _interpret_int(PINS_ENV_FEATURE_PREVIEW)
