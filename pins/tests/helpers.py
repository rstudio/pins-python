import pytest

import contextlib
import uuid
import os
import json
import shutil

from tempfile import TemporaryDirectory
from pathlib import Path
from functools import wraps
from datetime import datetime
from importlib_resources import files

from pins.boards import BaseBoard, BoardRsConnect
from fsspec import filesystem

DEFAULT_CREATION_DATE = datetime(2020, 1, 13, 23, 58, 59)

RSC_SERVER_URL = "http://localhost:3939"
# TODO: should use pkg_resources for this path?
RSC_KEYS_FNAME = "pins/tests/rsconnect_api_keys.json"

BOARD_CONFIG = {
    "file": {"path": ["PINS_TEST_FILE__PATH", None]},
    "s3": {"path": ["PINS_TEST_S3__PATH", "ci-pins"]},
    "gcs": {"path": ["PINS_TEST_GCS__PATH", "ci-pins"]},
    "rsc": {"path": ["PINS_TEST_RSC__PATH", RSC_SERVER_URL]},
    # TODO(question): R pins has the whole server a board
    # but it's a bit easier to test by (optionally) allowing a user
    # or something else to be a board
    # "rsc": {"path": ["PINS_TEST_RSC__PATH", ""]}
}

# TODO: Backend initialization should be independent of helpers, but these
#       high-level initializers are super handy.
#       putting imports inside rsconnect particulars for now


def rsc_from_key(name):
    from pins.rsconnect.api import RsConnectApi

    with open(RSC_KEYS_FNAME) as f:
        api_key = json.load(f)[name]
        return RsConnectApi(RSC_SERVER_URL, api_key)


def rsc_fs_from_key(name):
    from pins.rsconnect.fs import RsConnectFs

    rsc = rsc_from_key(name)

    return RsConnectFs(rsc)


def rsc_delete_user_content(rsc):
    guid = rsc.get_user()["guid"]
    content = rsc.get_content(owner_guid=guid)
    for entry in content:
        rsc.delete_content_item(entry["guid"])


def xfail_fs(*names):
    def outer(f):
        # Assumes a fixture named board is passed to the test
        @wraps(f)
        def wrapper(board, *args, **kwargs):
            if board.fs.protocol in names:
                pytest.xfail()
                return f(board, *args, **kwargs)
            else:
                return f(board, *args, **kwargs)

        return wrapper

    return outer


# Board Builders --------------------------------------------------------------


class BoardBuilder:
    """Handles the temporary creation, copying of data, and cleanup of boards.

    Note that builders can be configured by setting the environment variables
    specified in BOARD_CONFIG (e.g. PINS_TEST_S3__PATH). For the "file" filesystem
    precautions are taken to only use TemporaryDirectory.
    """

    def __init__(self, fs_name: str, path=None):
        config = BOARD_CONFIG[fs_name]

        if path is None:
            path = self.get_param_from_env(config["path"])

        if fs_name == "file":
            if path is not None:
                raise ValueError(
                    "path may not be defined for local 'file' filesystem. "
                    "It is set to a temporary directory for safety."
                )

            path = TemporaryDirectory()

        self.path = path
        self.fs_name = fs_name
        self.board_path_registry = []

    @staticmethod
    def get_param_from_env(entry):
        name, default = entry

        value = os.environ.get(name, default)

        if callable(value):
            return value()

        return value

    def create_tmp_board(self, src_board=None) -> BaseBoard:
        if self.fs_name == "gcs":
            opts = {"cache_timeout": 0}
        else:
            opts = {"listings_expiry_time": 0}

        fs = filesystem(self.fs_name, **opts)
        temp_name = str(uuid.uuid4())

        if isinstance(self.path, TemporaryDirectory):
            path_name = self.path.name
        else:
            path_name = self.path

        board_name = f"{path_name}/{temp_name}"

        if src_board is not None:
            fs.put(src_board, board_name, recursive=True)
        else:
            fs.mkdir(board_name)

        self.board_path_registry.append(board_name)
        return BaseBoard(board_name, fs=fs)

    def teardown_board(self, board):
        board.fs.rm(board.board, recursive=True)

    def teardown(self):
        # cleanup all temporary boards
        fs = filesystem(self.fs_name)

        for board_path in self.board_path_registry:
            print(board_path)
            if fs.exists(board_path):
                fs.rm(board_path, recursive=True)

        # only delete the base directory if it is explicitly temporary
        if isinstance(self.path, TemporaryDirectory):
            self.path.cleanup()


class RscBoardBuilder(BoardBuilder):
    """A dumb version of a RSConnect board builder.

    This class constructs a board for the same user each time, and deletes that
    user's content on teardown.
    """

    # TODO: could loop back once initializing all boards is clear

    def __init__(self, fs_name, path=None, *args, **kwargs):
        self.fs_name = fs_name
        self.path = None

    def create_tmp_board(self, src_board=None):
        from pins.rsconnect.fs import PinBundleManifest  # noqa

        board = BoardRsConnect("", rsc_fs_from_key("derek"))

        if src_board is None:
            return board

        # otherwise, try to copy in existing board ---
        # we need to add a manifest to each pin, so copy contents into a tmp dir
        with TemporaryDirectory() as tmp_dir:
            p_root = tmp_dir / Path(Path(src_board).name)
            p_user = Path("derek")

            shutil.copytree(src_board, p_root)

            # note that glob order is technically arbitrary
            for pin_entry in sorted(p_root.glob("*/*"), key=lambda x: str(x)):
                # two key points:
                #   1. username is required when putting content bundles up
                #   2. the version must be removed.
                # e.g. put derek/my-content to create a new bundle
                rpath = str(p_user / pin_entry.parent.relative_to(p_root))

                # need to create a manifest
                board.fs.put(str(pin_entry), rpath, recursive=True)

        return board

    def teardown_board(self, board):
        rsc_delete_user_content(board.fs.api)

    def teardown(self):
        self.teardown_board(self.create_tmp_board())


# Snapshot ====================================================================


class Snapshot:
    def __init__(self, path: Path, snapshot_update: bool = False):
        # make sure that it is a subdirectory of the pins package
        assert str(path.absolute()).startswith(str(files("pins")))

        self.path = path
        self.snapshot_update = snapshot_update

    def assert_has_same_files(self, dst_dir):
        if self.snapshot_update:
            shutil.copytree(dst_dir, self.path, dirs_exist_ok=True)

        else:
            dst_files = list(p.relative_to(dst_dir) for p in Path(dst_dir).rglob("*"))
            src_files = list(p.relative_to(self.path) for p in self.path.rglob("*"))
            src_diff = set(src_files) - set(dst_files)
            dst_diff = set(dst_files) - set(src_files)

            if src_diff:
                raise AssertionError(f"Snapshot has etra files: {src_diff}")

            if dst_diff:
                raise AssertionError(f"Snapshot missing files: {dst_diff}")

            # src_files and dst_files match at this point, so arbitrarily
            # return one
            return src_files

    def assert_equal_dir(self, dst_dir):
        if self.snapshot_update:
            shutil.copytree(dst_dir, self.path, dirs_exist_ok=True)

        else:
            p_dst_dir = Path(dst_dir)
            fnames = self.assert_has_same_files(dst_dir)
            for fname in fnames:
                snap_file = self.path / fname
                dst_file = p_dst_dir / fname
                if snap_file.is_dir():
                    assert dst_file.is_dir()
                else:
                    assert snap_file.read_text() == dst_file.read_text()

    def assert_equal_file(self, dst_file):
        if self.snapshot_update:
            shutil.copy(dst_file, self.path)

        assert self.path.read_text() == Path(dst_file).read_text()


@contextlib.contextmanager
def rm_env(*args):
    """
    Temporarily set the process environment variables.


    """
    old_environ = dict(os.environ)
    for arg in args:
        if arg in os.environ:
            del os.environ[arg]

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)
