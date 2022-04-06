import os
import pytest

from pins.tests.helpers import rm_env
from pins import config


@pytest.fixture
def env_unset():
    with rm_env(
        config.PINS_ENV_DATA_DIR,
        config.PINS_ENV_CACHE_DIR,
        config.PINS_ENV_INSECURE_READ,
    ):
        yield


def test_allow_insecure_read_no_env(env_unset):
    assert config.get_allow_insecure_read(True) is True
    assert config.get_allow_insecure_read(False) is False


def test_allow_insecure_read_env_1(env_unset):
    os.environ[config.PINS_ENV_INSECURE_READ] = "1"

    assert config.get_allow_insecure_read(True) is True
    assert config.get_allow_insecure_read(False) is False
    assert config.get_allow_insecure_read(None) is True


def test_allow_insecure_read_env_0(env_unset):
    os.environ[config.PINS_ENV_INSECURE_READ] = "0"

    assert config.get_allow_insecure_read(True) is True
    assert config.get_allow_insecure_read(False) is False
    assert config.get_allow_insecure_read(None) is False
