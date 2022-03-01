import pytest
import json
import tempfile

from pathlib import Path
from requests.exceptions import HTTPError
from pins.rsconnect_api import (
    RsConnectApi,
    RsConnectFs,
    RsConnectApiRequestError,
    PinBundleManifest,
)

pytestmark = pytest.mark.rsc  # noqa


RSC_SERVER_URL = "http://localhost:3939"
# TODO: should use pkg_resources for this path?
RSC_KEYS_FNAME = "pins/tests/rsconnect_api_keys.json"

N_USERS = 3
ERROR_CODE_BAD_GUID = 3


def rsc_from_key(name):
    with open(RSC_KEYS_FNAME) as f:
        api_key = json.load(f)[name]
        return RsConnectApi(RSC_SERVER_URL, api_key)


def delete_user_content(rsc):
    guid = rsc.get_user()["guid"]
    content = rsc.get_content(owner_guid=guid)
    for entry in content:
        rsc.delete_content(entry["guid"])


# The two rsconnect sessions below tear down at different intervals.


@pytest.fixture(scope="session")
def rsc_admin():
    rsc_admin = rsc_from_key("admin")

    yield rsc_admin

    # attempt to clean up. since this uses methods that are tested below, it is
    # possible for the cleanup to fail. we mitigate the risk of this by ordering
    # the cleanup methods before any POST methods below. but we need to create
    # content to test deletion, so hope it works as intended during cleanup.
    delete_user_content(rsc_admin)


@pytest.fixture(scope="function")
def rsc_short():
    # tears down content after each test
    rsc_susan = rsc_from_key("susan")

    # delete any content that might already exist
    delete_user_content(rsc_susan)

    yield rsc_susan

    delete_user_content(rsc_susan)


@pytest.fixture
def fs_admin():
    return RsConnectFs(rsc_from_key("admin"))


@pytest.fixture
def fs_short(rsc_short):
    return RsConnectFs(rsc_short)


# RsConnectApi ----------------------------------------------------------------

# Methods used in fixture teardown ----


def test_rsconnect_api_get_user(rsc_admin):
    me = rsc_admin.get_user()
    assert me["username"] == "admin"


def test_rsconnect_get_content_empty(rsc_short):
    me = rsc_short.get_user()
    content = rsc_short.get_content(owner_guid=me["guid"])
    assert len(content) == 0


# User ----


def test_rsconnect_api_get_users(rsc_admin):
    users = rsc_admin.get_users()
    assert len(users) == N_USERS
    # sanity check that it's got details about users
    assert "guid" in users[0].keys()


def test_rsconnect_api_get_users_arg_prefix(rsc_admin):
    # get the user "susan"
    users = rsc_admin.get_users(prefix="susa")
    assert len(users) == 1
    assert users[0]["username"] == "susan"


def test_rsconnect_api_get_users_no_pagination(rsc_admin):
    # walking pages appends all result entries into a single list
    # so we need to access to the "results" entry.
    users = rsc_admin.get_users(walk_pages=False)
    assert len(users["results"]) == N_USERS

    # fields used for pagination cursors
    assert "current_page" in users
    assert "total" in users


# Content ----


def test_rsconnect_api_post_content_item(rsc_short):
    content = rsc_short.post_content_item("test-content", "acl")
    assert "guid" in content.keys()
    assert content["name"] == "test-content"


def test_rsconnect_api_post_content_item_fail(rsc_short):
    with pytest.raises(RsConnectApiRequestError):
        rsc_short.post_content_item("test bad content", "acl")


def test_rsconnect_api_patch_content_item(rsc_short):
    post_res = rsc_short.post_content_item("test-content", "acl", title="old title")
    patch_res = rsc_short.patch_content_item(post_res["guid"], title="new title")

    assert post_res["title"] == "old title"
    assert patch_res["title"] == "new title"


def test_rsconnect_get_content_item(rsc_short):
    post_res = rsc_short.post_content_item("test-content", "acl")
    get_res = rsc_short.get_content_item(post_res["guid"])
    assert post_res == get_res


def test_rsconnect_get_content_args(rsc_short):
    me = rsc_short.get_user()

    post_res = rsc_short.post_content_item("test-content", "acl")
    get_res_all = rsc_short.get_content(me["guid"], post_res["name"])

    assert len(get_res_all) == 1
    assert post_res == get_res_all[0]


def test_rsconnect_del_content_result(rsc_short):
    post_res = rsc_short.post_content_item("test-content", "acl")
    res = rsc_short.delete_content(post_res["guid"])
    assert res is None


def test_rsconnect_del_content_fails(rsc_short):
    rsc_short.post_content_item("test-content", "acl")

    with pytest.raises(RsConnectApiRequestError) as exc_info:
        rsc_short.delete_content("abc")

    err = exc_info.value.args[0]

    # see https://docs.rstudio.com/connect/api/#overview--api-error-codes
    assert err["code"] == ERROR_CODE_BAD_GUID
    assert "abc" in err["error"]


# Bundles ----


def create_content_bundle(rsc, guid, target_dir=None):
    # TODO: commit a bundle to this repo, and add another test for that
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = tmp_dir if target_dir is None else target_dir
        (Path(tmp_dir) / "index.html").write_text("<html><body>yo</body></html>")
        PinBundleManifest.add_manifest_to_directory(tmp_dir)
        return rsc.post_content_bundle(guid, tmp_dir)


def test_rsconnect_api_post_content_bundle(rsc_short):
    # create bundle
    content = rsc_short.post_content_item("test-content-bundle", "acl")
    post_bundle_res = create_content_bundle(rsc_short, content["guid"])

    assert "content_guid" in post_bundle_res
    assert "id" in post_bundle_res


def test_rsconnect_api_post_content_bundle_fail(rsc_short):
    # create bundle
    rsc_short.post_content_item("test-content-bundle", "acl")
    with pytest.raises(RsConnectApiRequestError) as exc_info:
        create_content_bundle(rsc_short, "A-BAD-GUID")

    err = exc_info.value.args[0]
    assert err["code"] == ERROR_CODE_BAD_GUID
    assert "A-BAD-GUID" in err["error"]


def test_rsconnect_api_get_content_bundle(rsc_short):
    # create bundle
    content = rsc_short.post_content_item("test-content-bundle", "acl")
    post_bundle_res = create_content_bundle(rsc_short, content["guid"])

    get_res = rsc_short.get_content_bundle(
        post_bundle_res["content_guid"], post_bundle_res["id"]
    )

    assert post_bundle_res == get_res


def test_rsconnect_api_get_content_bundle_archive(rsc_short):
    # TODO: should just use a committed archive
    import tarfile
    import filecmp
    from pathlib import Path

    content = rsc_short.post_content_item("test-content-bundle", "acl")
    bundle = create_content_bundle(rsc_short, content["guid"])

    # create temporary directories for content source and dest to download to ----
    with tempfile.TemporaryDirectory() as tmp_src, tempfile.TemporaryDirectory() as tmp_dst:
        create_content_bundle(rsc_short, content["guid"], tmp_src)

        # download .tar.gz archive to a temporary file and unzip ----
        with tempfile.NamedTemporaryFile() as tmp_file:
            rsc_short.get_content_bundle_archive(
                content["guid"], bundle["id"], tmp_file.name
            )

            tmp_file.file.close()

            tar = tarfile.open(tmp_file.name, "r:gz")
            tar.extractall(path=tmp_dst)
            tar.close()

            # run checks inside context handler for easier de-bugging ----
            p_src = Path(tmp_src)
            p_dst = Path(tmp_dst)

            # test that archives both have identical index.html and manifest.json
            dst_fnames = list(p_dst.glob("*"))
            assert len(dst_fnames) == 2
            assert filecmp.cmp(p_src / "index.html", p_dst / "index.html")
            assert filecmp.cmp(p_src / "manifest.json", p_dst / "manifest.json")


# Tasks ----


def test_rsconnect_api_poll_tasks(rsc_short):
    # TODO: snippet gets repeated a lot
    content = rsc_short.post_content_item("test-content-bundle", "acl")
    bundle = create_content_bundle(rsc_short, content["guid"])
    task = rsc_short.post_content_item_deploy(content["guid"], bundle["id"])

    res = rsc_short.poll_tasks(task["task_id"])

    assert res["finished"]
    assert res["code"] == 0


# Misc ----


def test_rsconnect_api_ping(rsc_admin):
    d = rsc_admin.misc_ping()
    assert d == {}


def test_rsconnect_api_misc_get_content_bundle_file(rsc_short):
    content = rsc_short.post_content_item("test-content-bundle", "acl")
    bundle = create_content_bundle(rsc_short, content["guid"])
    rsc_short.post_content_item_deploy(content["guid"], bundle["id"])

    with tempfile.NamedTemporaryFile() as tmp:
        rsc_short.misc_get_content_bundle_file(
            content["guid"], bundle["id"], "index.html", tmp.file
        )

        tmp.file.close()
        # TODO: could test contents more carefully
        assert "<html>" in Path(tmp.name).read_text()


def test_rsconnect_api_misc_get_content_bundle_file_fail(rsc_short):
    content = rsc_short.post_content_item("test-content-bundle", "acl")
    bundle = create_content_bundle(rsc_short, content["guid"])

    # TODO: assert something about result
    with pytest.raises(HTTPError), tempfile.TemporaryFile() as tmp:
        rsc_short.misc_get_content_bundle_file(
            content["guid"], bundle["id"], "NOT-IN-BUNDLE.txt", tmp
        )


# RsConnectFs -----------------------------------------------------------------


def test_rsconnect_fs_ls_user(fs_admin):
    assert fs_admin.ls("") == ["admin", "derek", "susan"]


def test_rsconnect_fs_ls_user_content(fs_short):
    content = fs_short.api.post_content_item("test-content", "acl")
    assert fs_short.ls("susan") == ["test-content"]

    res_detailed = fs_short.ls("susan", details=True)
    assert len(res_detailed) == 1
    assert res_detailed[0] == content


def test_rsconnect_fs_ls_user_content_bundles(fs_short):
    content = fs_short.api.post_content_item("test-content", "acl")
    bund1 = create_content_bundle(fs_short.api, content["guid"])
    bund2 = create_content_bundle(fs_short.api, content["guid"])

    bund_sorted = sorted([bund1, bund2], key=lambda x: x["id"])

    res = fs_short.ls("susan/test-content")
    assert len(res) == 2
    assert sorted(res) == [bund_sorted[0]["id"], bund_sorted[1]["id"]]

    res_detailed = fs_short.ls("susan/test-content", details=True)
    assert len(res_detailed) == 2

    res_sorted = sorted(res_detailed, key=lambda x: x["id"])
    assert res_sorted[0] == bund_sorted[0]
    assert res_sorted[1] == bund_sorted[1]


def test_rsconnect_fs_info(fs_short):
    # TODO: copied from above. lots of creating bundles in tests.
    content = fs_short.api.post_content_item("test-content", "acl")
    bund1 = create_content_bundle(fs_short.api, content["guid"])
    create_content_bundle(fs_short.api, content["guid"])

    res_user = fs_short.info("susan")
    assert res_user["username"] == "susan"

    res_content = fs_short.info("susan/test-content")
    assert res_content == content

    res_bundle = fs_short.info(f"susan/test-content/{bund1['id']}")
    assert res_bundle == bund1
