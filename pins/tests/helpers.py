import uuid
import os

from tempfile import TemporaryDirectory

from pins.boards import BaseBoard
from fsspec import filesystem

BOARD_CONFIG = {
    "file": {"path": ["PINS_TEST_FILE__PATH", None]},
    "s3": {"path": ["PINS_TEST_S3__PATH", "ci-pins"]},
    # TODO(question): R pins has the whole server a board
    # but it's a bit easier to test by (optionally) allowing a user
    # or something else to be a board
    # "rsc": {"path": ["PINS_TEST_RSC__PATH", ""]}
}


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

    def get_param_from_env(self, entry):
        name, default = entry

        value = os.environ.get(name, default)

        if callable(value):
            return value()

        return value

    def create_tmp_board(self, src_board=None) -> BaseBoard:
        fs = filesystem(self.fs_name)
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
