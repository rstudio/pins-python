import pytest
import tempfile

from datetime import datetime
from io import StringIO

from pins.meta import Meta, MetaFactory
from pins.versions import Version

META_DEFAULTS = {
    "title": "some title",
    "description": "some description",
    "file": "some_file.csv",
    "file_size": 3,
    "pin_hash": "abcdef",
    "created": "20001230T124647Z",
    "type": "csv",
    "api_version": 1,
    "version": Version(datetime(2000, 12, 30, 12, 46, 47), "abcdef"),
}


@pytest.fixture
def meta():
    return Meta(**META_DEFAULTS)


@pytest.mark.xfail
def test_meta_to_dict_is_recursive(meta):
    d_meta = meta.to_dict()
    assert d_meta["version"] == meta.version.to_dict()


def test_meta_to_pin_dict_roundtrip(meta):
    d_meta = meta.to_pin_dict()
    meta2 = Meta.from_pin_dict(d_meta, meta.version)
    assert meta == meta2


def test_meta_factory_create():
    mf = MetaFactory()
    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_file.file.write(b"test")
        tmp_file.file.close()

        kwargs = {
            "title": "some title",
            "description": "some description",
            "user": {},
            "type": "csv",
            "name": "some_name",
        }

        meta = mf.create(tmp_file.name, **kwargs)

        # test that kwargs are passed through ----
        for k, v in kwargs.items():
            assert getattr(meta, k) == v

        # test calculated fields ----
        # TODO(compat): should append suffix to name attr (like in R pins)?
        #       otherwise, will break cross compat?
        assert meta.file == "some_name"
        assert meta.file_size == 4


def test_meta_factory_read_yaml_roundtrip(meta):
    pin_yaml = meta.to_yaml()

    mf = MetaFactory()
    meta2 = mf.read_yaml(StringIO(pin_yaml), meta.version)

    assert meta == meta2
