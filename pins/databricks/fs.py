import shutil
from io import BytesIO
from pathlib import Path, PurePath

from fsspec import AbstractFileSystem

from pins.errors import PinsError


class DatabricksFs(AbstractFileSystem):
    protocol = "dbc"

    def ls(self, path, detail=False, **kwargs):
        return self._databricks_ls(path, detail)

    def exists(self, path: str, **kwargs):
        return self._databricks_exists(path)

    def open(self, path: str, mode: str = "rb", *args, **kwargs):
        if mode != "rb":
            raise NotImplementedError
        return self._databricks_open(path)

    def get(self, rpath, lpath, recursive=False, **kwargs):
        self._databricks_get(rpath, lpath, recursive, **kwargs)

    def mkdir(self, path, create_parents=True, **kwargs):
        if not create_parents:
            raise NotImplementedError
        self._databricks_mkdir(path)

    def put(
        self,
        lpath,
        rpath,
        recursive=True,
        maxdepth=None,
        **kwargs,
    ):
        if not recursive:
            raise NotImplementedError
        if maxdepth is not None:
            raise NotImplementedError
        self._databricks_put(lpath, rpath)

    def rm(self, path, recursive=True, maxdepth=None) -> None:
        if not recursive:
            raise NotImplementedError
        if maxdepth is not None:
            raise NotImplementedError
        if self._databricks_exists(path):
            self._databricks_rm_dir(path)

    @staticmethod
    def _databricks_put(lpath, rpath):
        try:
            from databricks.sdk import WorkspaceClient
        except ModuleNotFoundError:
            raise PinsError(
                "Install the `databricks-sdk` package for Databricks board support."
            )

        w = WorkspaceClient()
        path = Path(lpath).absolute()
        orig_path = path

        def _upload_files(path):
            contents = Path(path)
            for item in contents.iterdir():
                abs_path = PurePath(path).joinpath(item)
                is_file = Path(abs_path).is_file()
                if is_file:
                    rel_path = abs_path.relative_to(orig_path)
                    db_path = PurePath(rpath).joinpath(rel_path)
                    file = open(abs_path, "rb")
                    w.files.upload(str(db_path), BytesIO(file.read()), overwrite=True)
                else:
                    _upload_files(abs_path)

        _upload_files(path)

    def _databricks_get(self, board, rpath, lpath, recursive=False, **kwargs):
        try:
            from databricks.sdk import WorkspaceClient
        except ModuleNotFoundError:
            raise PinsError(
                "Install the `databricks-sdk` package for Databricks board support."
            )

        w = WorkspaceClient()
        file_type = self._databricks_is_type(rpath)
        if file_type == "file":
            board.fs.get(rpath, lpath, **kwargs)
            return

        def _get_files(path, recursive, **kwargs):
            raw_contents = w.files.list_directory_contents(path)
            contents = list(raw_contents)
            details = list(map(self._databricks_content_details, contents))
            for item in details:
                item_path = item.get("path")
                if item.get("is_directory"):
                    if recursive:
                        _get_files(item_path, recursive=recursive, **kwargs)
                else:
                    rel_path = PurePath(item_path).relative_to(rpath)
                    target_path = PurePath(lpath).joinpath(rel_path)
                    board.fs.get(item_path, str(target_path))

        _get_files(rpath, recursive, **kwargs)

    def _databricks_open(self, path):
        try:
            from databricks.sdk import WorkspaceClient
        except ModuleNotFoundError:
            raise PinsError(
                "Install the `databricks-sdk` package for Databricks board support."
            )

        if not self._databricks_exists(path):
            raise PinsError("File or directory does not exist")
        w = WorkspaceClient()
        resp = w.files.download(path)
        f = BytesIO()
        shutil.copyfileobj(resp.contents, f)
        f.seek(0)
        return f

    def _databricks_exists(self, path: str):
        if self._databricks_is_type(path) == "nothing":
            return False
        else:
            return True

    @staticmethod
    def _databricks_is_type(path: str):
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.errors import NotFound
        except ModuleNotFoundError:
            raise PinsError(
                "Install the `databricks-sdk` package for Databricks board support."
            )

        w = WorkspaceClient()
        try:
            w.files.get_metadata(path)
        except NotFound:
            try:
                w.files.get_directory_metadata(path)
            except NotFound:
                return "nothing"
            else:
                return "directory"
        else:
            return "file"

    def _databricks_ls(self, path, detail):
        try:
            from databricks.sdk import WorkspaceClient
        except ModuleNotFoundError:
            raise PinsError(
                "Install the `databricks-sdk` package for Databricks board support."
            )

        if not self._databricks_exists(path):
            raise PinsError("File or directory does not exist")
        w = WorkspaceClient()
        if self._databricks_is_type(path) == "file":
            if detail:
                return [dict(name=path, size=None, type="file")]
            else:
                return path

        contents_raw = w.files.list_directory_contents(path)
        contents = list(contents_raw)
        items = []
        for item in contents:
            item = self._databricks_content_details(item)
            item_path = item.get("path")
            item_path = item_path.rstrip("/")
            if detail:
                if item.get("is_directory"):
                    item_type = "directory"
                else:
                    item_type = "file"
                items.append(dict(name=item_path, size=None, type=item_type))
            else:
                items.append(item_path)
        return items

    def _databricks_rm_dir(self, path):
        try:
            from databricks.sdk import WorkspaceClient
        except ModuleNotFoundError:
            raise PinsError(
                "Install the `databricks-sdk` package for Databricks board support."
            )

        w = WorkspaceClient()
        raw_contents = w.files.list_directory_contents(path)
        contents = list(raw_contents)
        details = list(map(self._databricks_content_details, contents))
        for item in details:
            item_path = item.get("path")
            if item.get("is_directory"):
                self._databricks_rm_dir(item_path)
            else:
                w.files.delete(item_path)
        w.files.delete_directory(path)

    @staticmethod
    def _databricks_mkdir(path):
        try:
            from databricks.sdk import WorkspaceClient
        except ModuleNotFoundError:
            raise PinsError(
                "Install the `databricks-sdk` package for Databricks board support."
            )

        w = WorkspaceClient()
        w.files.create_directory(path)

    @staticmethod
    def _databricks_content_details(item):
        details = {
            "path": item.path,
            "name": item.name,
            "is_directory": item.is_directory,
        }
        return details
