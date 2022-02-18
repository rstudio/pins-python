import pytest
import json
from pins.rsconnect_api import RsConnectApi, RsConnectFs

RSC_SERVER_URL = "http://localhost:3939"
# TODO: should use pkg_resources for this path?
RSC_KEYS_FNAME = "pins/tests/rsconnect_api_keys.json"


def rsc_from_key(name):
    with open(RSC_KEYS_FNAME) as f:
        api_key = json.load(f)[name]
        return RsConnectApi(RSC_SERVER_URL, api_key)


@pytest.fixture
def rs_admin():
    return rsc_from_key("admin")


@pytest.fixture
def fs_admin():
    return RsConnectFs(rsc_from_key("admin"))


def test_rsconnect_api_get_user(rs_admin):
    me = rs_admin.get_user()
    assert me["username"] == "admin"


def test_rsconnect_api_ping(rs_admin):
    d = rs_admin.ping()
    assert d == {}


def test_rsconnect_api_get_users(rs_admin):
    rs_admin.get_users()


def test_rsconnect_fs_ls(fs_admin):
    assert fs_admin.ls("") == ["admin", "derek", "susan"]
