from databricks.sdk import WorkspaceClient
from fsspec import AbstractFileSystem
from typing import ClassVar

class DatabricksFs(AbstractFileSystem):
    protocol: ClassVar[str | tuple[str, ...]] = "dbc"

    def __init__(self, folder_url, **kwargs):
        self.folder_url = folder_url

    def ls(self, path, details=False, **kwargs):
        w = WorkspaceClient()
        all_items = []
        for item in w.files.list_directory_contents(self.folder_url):
            all_items.append(item.name)
        return all_items
