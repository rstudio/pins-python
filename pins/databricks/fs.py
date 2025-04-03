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
        files = _map_folder(path=path, recurse=False)
        items = []
        for file in files:
            name = file.get("name")
            if(detail):
                if(file.get("is_directory")):
                    type = "directory"
                else:
                    type = "file"
                items.append(dict(name = name, size = None, type = type)) 
            else:
                items.append(name)
        return items

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
        exists  = self.exists(path) 
        if(exists):
            lev1 = self._list_dir(path)
            for item1 in lev1:
                if item1.get("is_directory"):
                    lev2 = self._list_dir(item1.get("path"))
                    for item2 in lev2:
                        if item1.get("is_directory"):       
                            lev3 = self._list_dir(item2.get("path"), "path")
                            for item3 in lev3:
                                self.workspace.files.delete(item3)
                            self.workspace.files.delete_directory(item2.get("path"))
                        else:
                            self.workspace.files.delete(item2.get("path"))                         
                    self.workspace.files.delete_directory(item1.get("path"))
                else:
                    self.workspace.files.delete(item1.get("path"))
            self.workspace.files.delete_directory(path)


    def _list_dir(self, path, field="all"):
        dir_contents = list(self.workspace.files.list_directory_contents(path))
        details = list(map(_map_details, dir_contents))
        if field != "all":
            items = []
            for item in details:
                items.append(item.get(field))
        else:
            items = details
        return items

def _map_folder(path, recurse=True, include_folders=True, include_files=True):
    w = WorkspaceClient()
    dir_contents = list(w.files.list_directory_contents(path))
    details = list(map(_map_details, dir_contents))
    items = []
    for item in details:        
        if(item.get("is_directory")):
            if(include_folders):
                items = items + [item]
            if(recurse):
                more_details = _map_folder(
                    path = item.get("path"), 
                    recurse=True, 
                    include_folders=include_folders,
                    include_files=include_files
                    )
                items = items + more_details
        else:
            if(include_files):
                items = items + [item]
    return items

def _map_details(item):
    details = {
        "path": item.path,
        "name": item.name,
        "is_directory": item.is_directory,
    }
    return details