import pytest

from datetime import datetime

from pins.meta import Meta
from pins.versions import Version

META_DEFAULTS = {
    "title": "some title",
    "description": "some description",
    "file": "some_file.csv",
    "file_size": 3,
    "type": "csv",
    "api_version": 1,
    "version": Version(datetime(2000, 12, 30, 12, 46, 47), "abcdef"),
}


@pytest.fixture
def meta():
    return Meta(**META_DEFAULTS)


def test_meta_to_dict_is_recursive(meta):
    d_meta = meta.to_dict()
    assert d_meta["version"] == meta.version.to_dict()


def test_meta_to_pin_dict_roundtrip(meta):
    d_meta = meta.to_pin_dict()
    meta2 = Meta.from_pin_dict(d_meta, Version)
    assert meta == meta2
