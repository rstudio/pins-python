from datetime import datetime
from io import BytesIO

import pytest
import xxhash

from pins.errors import PinsVersionError
from pins.versions import Version

EXAMPLE_DATE = datetime(2021, 1, 2, 13, 58, 59)


@pytest.fixture
def bytes_():
    return BytesIO(b"123"), xxhash.xxh64(b"123").hexdigest()


def test_version_from_string():
    version = Version.from_string("20220209T220116Z-baf3f")
    assert str(version.created) == "2022-02-09 22:01:16"
    assert version.hash == "baf3f"


def test_version_from_string_too_many_hyphens():
    with pytest.raises(
        PinsVersionError, match="version string can only have 1 '-', but contains 2"
    ):
        Version.from_string("20220209T220116Z-baf3f-")


def test_version_from_string_too_few_hyphens():
    with pytest.raises(
        PinsVersionError, match="version string can only have 1 '-', but contains 0"
    ):
        Version.from_string("20220209T220116Zbaf3f")
def test_version_from_string_baddate():
    with pytest.raises(PinsVersionError, match="Invalid date part of version: bug"):
        Version.from_string("bug-baf3f")


def test_version_hash_file(bytes_):
    f_bytes, digest = bytes_
    assert Version.hash_file(f_bytes) == digest


def test_version_from_files(bytes_):
    f_bytes, digest = bytes_
    v = Version.from_files([f_bytes], EXAMPLE_DATE)

    assert v.hash == digest
    assert v.created == EXAMPLE_DATE
