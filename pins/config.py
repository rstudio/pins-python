import appdirs
import os

PINS_NAME = "pins-py"
PINS_ENV_DATA_DIR = "PINS_DATA_DIR"
PINS_ENV_CACHE_DIR = "PINS_CACHE_DIR"
PINS_ENV_INSECURE_READ = "PINS_ALLOW_PICKLE_READ"


def get_data_dir():
    return os.environ.get(PINS_ENV_DATA_DIR, appdirs.user_data_dir(PINS_NAME))


def get_cache_dir():
    return os.environ.get(PINS_ENV_CACHE_DIR, appdirs.user_cache_dir(PINS_NAME))


def get_allow_pickle_read(flag):
    if flag is None:
        env_var = os.environ.get(PINS_ENV_INSECURE_READ, "0")
        try:
            env_int = int(env_var)
        except ValueError:
            raise ValueError(
                f"{PINS_ENV_INSECURE_READ} must be '0' or '1', but was set to "
                f"{repr(env_var)}."
            )

        flag = bool(env_int)

    return flag


def _enable_logs():
    import logging

    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handlers = [logging.FileHandler("filename.log"), logging.StreamHandler()]

    logging.basicConfig(level=logging.INFO, format=format, handlers=handlers)
