import pytest

from pytest import mark as m
from pins.tests.helpers import BoardBuilder

# Based on https://github.com/machow/siuba/blob/main/siuba/tests/helpers.py
BACKEND_MARKS = ["fs_s3", "fs_file"]


params_backend = [
    pytest.param(lambda: BoardBuilder("file"), id="file", marks=m.fs_file),
    pytest.param(lambda: BoardBuilder("s3"), id="s3", marks=m.fs_s3),
]


@pytest.fixture(params=params_backend, scope="session")
def backend(request):
    backend = request.param()
    yield backend
    backend.teardown()


def pytest_configure(config):
    # TODO: better way to define all marks? Can we iterate over params above?
    for mark_name in BACKEND_MARKS:
        fs_name = mark_name.split("_")[-1]
        config.addinivalue_line(
            "markers", f"{mark_name}: mark test to only run on {fs_name} filesystem."
        )

    # TODO: once RStudioConnect backend added, can remove this line
    config.addinivalue_line("markers", "rsc: mark test to only run on rsc filesystem.")
