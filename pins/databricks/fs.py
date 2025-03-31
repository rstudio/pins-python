import os
import shutil
from io import BytesIO
from typing import ClassVar
from fsspec import AbstractFileSystem
from databricks.sdk import WorkspaceClient


class DatabricksFs(AbstractFileSystem):
    protocol: ClassVar[str | tuple[str, ...]] = "dbc"

    def __init__(self, folder_url, **kwargs):
        self.folder_url = folder_url
        self.workspace = WorkspaceClient()

    def ls(self, path, details=False, **kwargs):
        return self._list_items(path)

    def exists(self, path: str, **kwargs):
        path = os.path.basename(path)
        return path in self._list_items(self.folder_url)

    def open(self, path: str, mode: str = "rb", *args, **kwargs):
        resp = self.workspace.files.download(path)
        f = BytesIO()
        shutil.copyfileobj(resp.contents, f)
        f.seek(0)
        return f

    def mkdir(self, path, create_parents=True, **kwargs):
        if not create_parents:
            raise NotImplementedError
        self.workspace.files.create_directory(path)

    def put(
        self,
        lpath,
        rpath,
        recursive=True,
        maxdepth=None,
        **kwargs,
    ):
        for item in os.listdir(lpath):
            abs_item = os.path.join(lpath, item)
            if os.path.isfile(abs_item):
                dest = os.path.join(rpath, item)
                file = open(abs_item, "rb")
                self.workspace.files.upload(dest, BytesIO(file.read()), overwrite=True)

    def _list_items(self, path):
        dir_contents = list(self.workspace.files.list_directory_contents(path))
        all_items = []
        for item in dir_contents:
            all_items.append(item.name)
        return all_items
