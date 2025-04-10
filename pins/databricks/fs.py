import os
import shutil
from io import BytesIO
from typing import ClassVar
from fsspec import AbstractFileSystem
from databricks.sdk import WorkspaceClient
from pins.errors import PinsError

class DatabricksFs(AbstractFileSystem):
    protocol: ClassVar[str | tuple[str, ...]] = "dbc"

    def ls(self, path, detail=False, **kwargs):        
        return _databricks_ls(path, detail)

    def exists(self, path: str, **kwargs):
        return _databricks_exists(path)

    def open(self, path: str, mode: str = "rb", *args, **kwargs):
        if mode != "rb":
            raise NotImplementedError        
        return _databricks_open(path)
    
    def get(self, rpath, lpath, recursive=False, **kwargs):
        _databricks_get(self, rpath, lpath, recursive, **kwargs)

    def mkdir(self, path, create_parents=True, **kwargs):
        if not create_parents:
            raise NotImplementedError
        _databricks_mkdir(path)

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
        if not recursive:
            raise NotImplementedError
        if maxdepth is not None:
            raise NotImplementedError
        if(_databricks_exists(path)):
            _databricks_rm_dir(path)

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
            if(is_file):     
                rel_path = os.path.relpath(abs_path, orig_path)
                db_path = os.path.join(rpath, rel_path)                
                file = open(abs_path, "rb")
                w.files.upload(db_path, BytesIO(file.read()), overwrite=True)
            else:
                _upload_files(abs_path)
    _upload_files(path)

def _databricks_get(board, rpath, lpath, recursive = False, **kwargs):
    w = WorkspaceClient()
    file_type = _databricks_is_type(rpath)
    if(file_type == "file"):
        board.fs.get(rpath, lpath, **kwargs)
        return
    def _get_files(path, recursive, **kwargs):
        raw_contents = w.files.list_directory_contents(path)
        contents = list(raw_contents)
        details = list(map(_databricks_content_details, contents))    
        for item in details:     
            item_path = item.get("path")   
            if(item.get("is_directory")):
                if(recursive):
                    _get_files(item_path, recursive = recursive, **kwargs)      
            else:
                rel_path = os.path.relpath(item_path, rpath)
                target_path = os.path.join(lpath, rel_path)
                board.fs.get(item_path, target_path)
    _get_files(rpath, recursive, **kwargs)

def _databricks_open(path):
    if(_databricks_exists(path) == False):
        raise PinsError("File or directory does not exist")
    w = WorkspaceClient()
    resp = w.files.download(path)
    f = BytesIO()
    shutil.copyfileobj(resp.contents, f)
    f.seek(0)
    return f

def _databricks_exists(path: str):
    if(_databricks_is_type(path) == "nothing"):
        return False
    else:
        return True

def _databricks_is_type(path: str):
    w = WorkspaceClient()
    try:
        w.files.get_metadata(path)
    except:
        try:
            w.files.get_directory_metadata(path) 
        except: 
            return "nothing"
        else:
            return "directory"
    else:
        return "file"        

def _databricks_ls(path, detail):
    if(_databricks_exists(path) == False):
        raise PinsError("File or directory does not exist")    
    w = WorkspaceClient()
    if(_databricks_is_type(path) == "file"):
        if(detail):
            return [dict(name = path, size = None, type = "file")]
        else:
            return path

    contents_raw = w.files.list_directory_contents(path)
    contents = list(contents_raw)
    items = []
    for item in contents:
        item = _databricks_content_details(item)
        item_path = item.get("path")
        item_path = item_path.rstrip("/")
        if(detail):            
            if(item.get("is_directory")):
                type = "directory"                
            else:
                type = "file"
            items.append(dict(name = item_path, size = None, type = type)) 
        else:
            items.append(item_path)
    return items

def _databricks_rm_dir(path):
    w = WorkspaceClient()
    raw_contents = w.files.list_directory_contents(path)
    contents = list(raw_contents)
    details = list(map(_databricks_content_details, contents))
    items = []
    for item in details:     
        item_path = item.get("path")   
        if(item.get("is_directory")):
            _databricks_rm_dir(item_path)      
        else:
            w.files.delete(item_path)
    w.files.delete_directory(path)

def _databricks_mkdir(path):
    w = WorkspaceClient()
    w.files.create_directory(path)    

def _databricks_content_details(item):
    details = {
        "path": item.path,
        "name": item.name,
        "is_directory": item.is_directory,
    }
    return details
