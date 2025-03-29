import os
from databricks.sdk import WorkspaceClient
from fsspec import AbstractFileSystem
from typing import ClassVar

class DatabricksFs(AbstractFileSystem):
    protocol: ClassVar[str | tuple[str, ...]] = "dbc"

    def __init__(self, folder_url, **kwargs):
        self.folder_url = folder_url

    def ls(self, path, details=False, **kwargs):
        return self._list_folders(path)

    def exists(self, path: str, **kwargs):
        path = os.path.basename(path)
        return path in self._list_folders(self.folder_url)

    def _list_folders(self, path):
        w = WorkspaceClient()
        dir_contents = list(w.files.list_directory_contents(path))
        all_folders = []
        for item in dir_contents:
            if(item.is_directory):
                all_folders.append(item.name)
        return all_folders                 
