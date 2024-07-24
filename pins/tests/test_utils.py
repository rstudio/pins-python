import pytest

from pins.config import pins_options
from pins.utils import inform


@pytest.fixture
def quiet():
    orig = pins_options.quiet
    pins_options.quiet = True
    yield
    pins_options.quiet = orig


def test_inform(capsys):
    msg = "a message"
    inform(None, msg)
    captured = capsys.readouterr()
    assert captured.err == msg + "\n"


def test_inform_quiet(quiet, capsys):
    inform(None, "a message")
    captured = capsys.readouterr()
    assert captured.err == ""
