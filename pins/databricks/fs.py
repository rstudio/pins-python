import os
import shutil
from io import BytesIO
from typing import ClassVar
from fsspec import AbstractFileSystem
from databricks.sdk import WorkspaceClient


class DatabricksFs(AbstractFileSystem):
    protocol: ClassVar[str | tuple[str, ...]] = "dbc"

    def __init__(self, folder_url, **kwargs):
        self.workspace = WorkspaceClient()

    def ls(self, path, detail=False, **kwargs):
        files = self._list_dir(path, "name")
        if(detail):
            all_files = []
            for file in files:
                all_files.append(dict(name = file, size = None, type = "file"))    
            return all_files 
        else:
            return files

    def exists(self, path: str, **kwargs):
        file_exists = True
        try:
            self.workspace.files.get_metadata(path)
        except:
            file_exists = False

        dir_exists = True
        try:
            self.workspace.files.get_directory_metadata(path)
        except:
            dir_exists = False

        return file_exists | dir_exists

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

    def rm(self, path, recursive=True, maxdepth=None) -> None:
        lev1 = self._list_dir(path)
        for item1 in lev1:
            if item1.get("is_directory"):
                lev2 = self._list_dir(item1.get("path"), "path")
                for item2 in lev2:
                    self.workspace.files.delete(item2)
                self.workspace.files.delete_directory(item1.get("path"))
            else:
                self.workspace.files.delete(item1.get("path"))
        self.workspace.files.delete_directory(path)

    def _map_details(self, item):
        details = {
            "path": item.path,
            "name": item.name,
            "is_directory": item.is_directory,
        }
        return details

    def _list_dir(self, path, field="all"):
        dir_contents = list(self.workspace.files.list_directory_contents(path))
        details = list(map(self._map_details, dir_contents))
        if field != "all":
            items = []
            for item in details:
                items.append(item.get(field))
        else:
            items = details
        return items
