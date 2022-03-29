import time
import shutil

import pytest
from pins.cache import CachePruner, touch_access_time, cache_prune


@pytest.fixture
def some_file(tmp_dir2):
    p = tmp_dir2 / "some_file.txt"
    p.touch()
    return p


def test_touch_access_time_manual(some_file):
    some_file.stat().st_atime

    access_time = time.time() - 60 * 60 * 24
    touch_access_time(some_file, access_time)

    assert some_file.stat().st_atime == access_time


def test_touch_access_time_auto(some_file):
    orig_access = some_file.stat().st_atime

    time.sleep(0.2)
    new_time = touch_access_time(some_file)

    assert some_file.stat().st_atime == new_time
    assert orig_access < new_time


# Cache pruning ===============================================================


@pytest.fixture
def a_cache(tmp_dir2):
    return tmp_dir2


def create_metadata(p, access_time):
    p.mkdir(parents=True, exist_ok=True)
    meta = p / "data.txt"
    meta.touch()
    touch_access_time(meta, access_time)


@pytest.fixture
def pin1_v1(a_cache):  # current
    v1 = a_cache / "a_pin" / "version_1"
    create_metadata(v1, time.time())

    return v1


@pytest.fixture
def pin1_v2(a_cache):
    v2 = a_cache / "a_pin" / "version_2"
    create_metadata(v2, time.time() - 60 * 60 * 24)  # one day ago

    return v2


@pytest.fixture
def pin2_v3(a_cache):
    v3 = a_cache / "other_pin" / "version_3"
    create_metadata(v3, time.time() - 60 * 60 * 48)  # two days ago

    return v3


def test_cache_pruner_old_versions_none(a_cache, pin1_v1):
    pruner = CachePruner(a_cache)

    old = pruner.old_versions(days=1)

    assert len(old) == 0


def test_cache_pruner_old_versions_days0(a_cache, pin1_v1):
    pruner = CachePruner(a_cache)
    old = pruner.old_versions(days=0)

    assert len(old) == 1
    assert old[0] == pin1_v1


def test_cache_pruner_old_versions_some(a_cache, pin1_v1, pin1_v2):
    # create: tmp_dir/pin1/version1

    pruner = CachePruner(a_cache)

    old = pruner.old_versions(days=1)

    assert len(old) == 1
    assert old[0] == pin1_v2


def test_cache_pruner_old_versions_multi_pins(a_cache, pin1_v2, pin2_v3):
    pruner = CachePruner(a_cache)
    old = pruner.old_versions(days=1)

    assert len(old) == 2
    assert set(old) == {pin1_v2, pin2_v3}


def test_cache_prune_prompt(tmp_dir2, a_cache, pin1_v1, pin2_v3, monkeypatch):
    p_board = tmp_dir2 / "a_board"
    shutil.copytree(a_cache, p_board)
    cache_prune(days=1, cache_root=tmp_dir2, prompt=False)

    versions = list(p_board.glob("*/*"))

    # pin2_v3 deleted
    assert len(versions) == 1
