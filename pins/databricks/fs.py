import os
from databricks.sdk import WorkspaceClient
from fsspec import AbstractFileSystem
from typing import ClassVar
from io import BytesIO
import shutil

class DatabricksFs(AbstractFileSystem):
    protocol: ClassVar[str | tuple[str, ...]] = "dbc"

    def __init__(self, folder_url, **kwargs):
        self.folder_url = folder_url
        self.workspace = w = WorkspaceClient()

    def ls(self, path, details=False, **kwargs):
        return self._list_folders(path)

    def exists(self, path: str, **kwargs):
        path = os.path.basename(path)
        return path in self._list_folders(self.folder_url)

    def open(self, path: str, mode: str = "rb", *args, **kwargs):
        resp = self.workspace.files.download(path) 
        f = BytesIO()
        shutil.copyfileobj(resp.contents, f)
        f.seek(0)
        return f

    def _list_folders(self, path):
        dir_contents = list(self.workspace.files.list_directory_contents(path))
        all_folders = []
        for item in dir_contents:
            if(item.is_directory):
                all_folders.append(item.name)
        return all_folders                 
