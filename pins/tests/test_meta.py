import tempfile
from datetime import datetime
from io import StringIO

import pytest
import yaml

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
    meta2 = Meta.from_pin_dict(d_meta, meta.name, meta.version)
    assert meta == meta2


def test_meta_unknown_fields():
    m = Meta(**META_DEFAULTS, unknown_fields={"some_other_field": 1})

    assert m.some_other_field == 1

    with pytest.raises(AttributeError):
        m.should_not_exist_here

    assert "unknown_fields" not in m.to_pin_dict()
    assert "some_other_field" not in m.to_pin_dict()


def test_meta_factory_create():
    mf = MetaFactory()
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_file = f"{tmp_dir}/some_name"
        with open(tmp_file, "wb") as f:
            f.write(b"test")

        kwargs = {
            "title": "some title",
            "description": "some description",
            "user": {},
            "type": "csv",
            "name": "some_name",
        }

        meta = mf.create(tmp_dir, tmp_file, **kwargs)

        # test that kwargs are passed through ----
        for k, v in kwargs.items():
            assert getattr(meta, k) == v

        # test calculated fields ----
        # TODO(compat): should append suffix to name attr (like in R pins)?
        #       otherwise, will break cross compat?
        assert meta.file == "some_name"
        assert meta.file_size == 4


def test_meta_factory_read_yaml_roundtrip(meta):
    pin_yaml = meta.to_pin_yaml()

    mf = MetaFactory()
    meta2 = mf.read_pin_yaml(StringIO(pin_yaml), meta.name, meta.version)

    assert meta == meta2


def test_meta_factory_roundtrip_unknown(meta):
    meta_dict = meta.to_pin_dict()
    meta_dict["some_other_field"] = 1

    pin_yaml = yaml.dump(meta_dict)

    mf = MetaFactory()

    meta2 = mf.read_pin_yaml(StringIO(pin_yaml), meta.name, meta.version)

    assert meta2 == meta
    assert meta2.some_other_field == 1
