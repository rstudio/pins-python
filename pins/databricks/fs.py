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
        return _databricks_exists(path)

    def open(self, path: str, mode: str = "rb", *args, **kwargs):
        if mode != "rb":
            raise NotImplementedError        
        return _databricks_open(path)

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
        if not recursive:
            raise NotImplementedError
        if maxdepth is not None:
            raise NotImplementedError
        _databricks_put(lpath, rpath)

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

def _databricks_put(lpath, rpath):
    w = WorkspaceClient()
    path = os.path.abspath(lpath)
    items = []
    orig_path = path
    def _upload_files(path):
        contents = os.listdir(path)
        for item in contents:        
            abs_path = os.path.join(path, item)
            is_file = os.path.isfile(abs_path)
            rel_path = os.path.relpath(abs_path, orig_path)
            db_path = os.path.join(rpath, rel_path)
            if(is_file):     
                file = open(abs_path, "rb")
                w.files.upload(db_path, BytesIO(file.read()), overwrite=True)
            else:
                _upload_files(abs_path)
    _upload_files(path)

def _databricks_open(path):
    w = WorkspaceClient()
    resp = w.files.download(path)
    f = BytesIO()
    shutil.copyfileobj(resp.contents, f)
    f.seek(0)
    return f

def _databricks_exists(path: str):
    w = WorkspaceClient()
    try:
        w.files.get_metadata(path)
    except:
        try:
            w.files.get_directory_metadata(path) 
        except: 
            return False
        else:
            return True
    else:
        return True

def _databricks_ls(path, detail):
    w = WorkspaceClient()
    contents_raw = w.files.list_directory_contents(path)
    contents = list(contents_raw)
    items = []
    for item in contents:
        item = _map_details(item)
        name = item.get("name")
        if(detail):
            if(item.get("is_directory")):
                type = "directory"
            else:
                type = "file"
            items.append(dict(name = name, size = None, type = type)) 
        else:
            items.append(name)
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
