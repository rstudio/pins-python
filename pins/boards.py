import tempfile
import shutil

from io import IOBase
from functools import cached_property
from pathlib import Path
from importlib_resources import files
from datetime import datetime

from typing import Protocol, Sequence, Optional, Mapping

from .versions import VersionRaw, guess_version
from .meta import Meta, MetaFactory
from .errors import PinsError
from .drivers import load_data, save_data, default_title


class IFileSystem(Protocol):
    def ls(self, path: str) -> Sequence[str]:
        ...

    def put(self) -> None:
        ...

    def open(self, path: str, mode: str, *args, **kwargs) -> IOBase:
        ...

    def get(self) -> None:
        ...

    def exists(self, path: str, **kwargs) -> bool:
        ...

    def mkdirs(self, path, create_parents=True, **kwargs) -> None:
        ...

    def rm(self, path, recursive=False, maxdepth=None) -> None:
        ...


class BaseBoard:
    def __init__(
        self, board: str, fs: IFileSystem, meta_factory=MetaFactory(),
    ):
        self.board = board
        self.fs = fs
        self.meta_factory = meta_factory

    def pin_exists(self, name: str) -> bool:
        """Determine if a pin exists.

        Parameters
        ----------
        name : str
        """

        return self.fs.exists(self.path_to_pin(name))

    def pin_versions(self, name: str, as_df: bool = True) -> Sequence[VersionRaw]:
        if not self.pin_exists(name):
            raise PinsError("Cannot check version, since pin %s does not exist" % name)

        versions_raw = self.fs.ls(self.path_to_pin(name))

        # get a list of Version(Raw) objects
        all_versions = []
        for full_path in versions_raw:
            version = self.keep_final_path_component(full_path)
            all_versions.append(guess_version(version))

        # sort them, with latest last
        sorted_versions = self.sort_pin_versions(all_versions)

        # TODO(defer): this deviates from R pins, which returns a df by default
        if as_df:
            import pandas as pd

            return pd.DataFrame([v.to_dict() for v in sorted_versions])

        return sorted_versions

    def pin_meta(self, name, version: str = None):
        pin_name = self.path_to_pin(name)

        # determine pin version -----------------------------------------------

        if version is not None:
            # ensure pin and version exist
            if not self.fs.exists(self.construct_path([pin_name, version])):
                raise PinsError(
                    f"Pin {name} either does not exist, "
                    f"or is missing version: {version}."
                )

            selected_version = guess_version(version)
        else:
            # otherwise, get the last pin version
            versions = self.pin_versions(name, as_df=False)

            if not len(versions):
                raise NotImplementedError("TODO: sanity check when no versions")

            # select last version ----
            selected_version = versions[-1]

        # fetch metadata for version ------------------------------------------

        components = [pin_name, selected_version.version]
        meta_name = self.meta_factory.get_meta_name(*components)

        path_version = self.construct_path([*components, meta_name])
        f = self.fs.open(path_version)
        return self.meta_factory.read_yaml(f, selected_version)

    def pin_list(self):
        full_paths = self.fs.ls(self.board)
        return list(map(self.keep_final_path_component, full_paths))

    def pin_fetch(self, name: str, version: Optional[str] = None) -> Meta:
        meta = self.pin_meta(name, version)

        # TODO: sanity check caching (since R pins does a cache touch here)
        # path = self.construct_path([self.board, name, version])
        # self.fs.get(...)

        # TODO: pin_fetch R lib uses this chance to cache the files
        #       need to ensure user can have a readable cache
        #       so they could pin_fetch and then examine the result, a la pin_download
        return meta

    def pin_read(self, name, version: Optional[str] = None, hash: Optional[str] = None):
        meta = self.pin_fetch(name, version)

        if hash is not None:
            raise NotImplementedError("TODO: validate hash")

        pin_name = self.path_to_pin(name)

        return load_data(
            meta, self.fs, self.construct_path([pin_name, meta.version.version])
        )

    def pin_write(
        self,
        x,
        name: Optional[str] = None,
        type: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Mapping] = None,
        versioned: Optional[bool] = None,
        created: Optional[datetime] = None,
    ):
        # TODO(compat): python pins added a created parameter above
        with tempfile.TemporaryDirectory() as tmp_dir:
            # create all pin data (e.g. data.txt, save object)
            meta = self.prepare_pin_version(
                tmp_dir, x, name, type, title, description, metadata, versioned, created
            )

            # move pin to destination ----
            # create pin version folder
            dst_pin_path = self.path_to_pin(name)
            dst_version_path = self.path_to_deploy_version(name, meta.version.version)

            try:
                self.fs.mkdir(dst_pin_path)
            except FileExistsError:
                pass

            # put tmp pin dir onto backend filesystem
            # TODO: if we allow the rsc backend to fs.exists("<user>/<content>/latest")
            #       and fs.put latest, then we don't have to check the two paths differ
            if self.fs.exists(dst_version_path) and dst_version_path != dst_pin_path:
                # note that we only raise an error if version path is a subdir
                # of the pin path.
                raise PinsError(
                    "Attempting to write pin version to {dst_version_path}, "
                    "but that directory already exists."
                )

            self.fs.put(tmp_dir, dst_version_path, recursive=True)

        return meta

    def validate_pin_name(self, name: str) -> None:
        if "/" in name:
            raise ValueError(f"Invalid pin name: {name}")

    def path_to_pin(self, name: str) -> str:
        self.validate_pin_name(name)

        return self.construct_path([self.board, name])

    def path_to_deploy_version(self, name: str, version: str):
        return self.construct_path([self.path_to_pin(name), version])

    def construct_path(self, elements) -> str:
        # TODO: should be the job of IFileSystem?
        return "/".join(elements)

    def keep_final_path_component(self, path):
        return path.split("/")[-1]

    def sort_pin_versions(self, versions):
        # assume filesystem returned them with most recent last
        return versions

    def prepare_pin_version(
        self,
        pin_dir_path,
        x,
        name: Optional[str] = None,
        type: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Mapping] = None,
        versioned: Optional[bool] = None,
        created: Optional[datetime] = None,
    ):
        if name is None:
            raise NotImplementedError("Name must be specified.")

        if versioned is False:
            raise NotImplementedError("Only writing versioned pins supported.")

        if type is None:
            raise NotImplementedError("Type argument is required.")

        if title is None:
            title = default_title(x, type)

        # create metadata from object on disk ---------------------------------
        # save all pin data to a temporary folder (including data.txt), so we
        # can fs.put it all straight onto the backend filesystem

        p_obj = Path(pin_dir_path) / name

        # file is saved locally in order to hash, calc size
        save_data(x, str(p_obj), type)

        meta = self.meta_factory.create(
            str(p_obj),
            type,
            title=title,
            description=description,
            user=metadata,
            name=name,
            created=created,
        )

        # write metadata to tmp pin folder
        meta_name = self.meta_factory.get_meta_name()
        src_meta_path = Path(pin_dir_path) / meta_name
        meta.to_yaml(src_meta_path.open("w"))

        return meta


class BoardRsConnect(BaseBoard):
    # TODO: note that board is unused in this class (e.g. it's not in construct_path())

    # TODO: should read template dynamically, not at class def'n time
    html_assets_dir: Path = files("pins") / "rsconnect/html"
    html_template: Path = files("pins") / "rsconnect/html/index.tpl"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # defaults work ----

    def pin_list(self):
        # lists all pin content on RStudio Connect server
        # we can't use fs.ls, because it will list *all content*
        paged_res = self.fs.api.misc_get_applications("content_type:pin")
        results = paged_res.results

        names = [f"{cont['owner_username']}/{cont['name']}" for cont in results]
        return names

    def validate_pin_name(self, name) -> None:
        if name.count("/") > 1:
            raise ValueError(f"Invalid pin name: {name}")

    def sort_pin_versions(self, versions) -> Sequence[VersionRaw]:
        # TODO: could alternatively implement a "select_pin_version" method
        # to be used by pin_meta
        return sorted(versions, key=lambda v: int(v.version))

    def path_to_pin(self, name: str) -> str:
        self.validate_pin_name(name)

        # pin of form "<user>/<content_name>" is fully specified
        if "/" in name:
            return name

        # otherwise, prepend username to pin
        return self.construct_path([self.user_name, name])

    def path_to_deploy_version(self, name: str, version: str):
        # RSConnect deploys a content bundle for a new version, so we simply need
        # to fs.put to the <user>/<content_name>.
        return self.path_to_pin(name)

    @cached_property
    def user_name(self):
        user = self.fs.api.get_user()
        return user["username"]

    def prepare_pin_version(self, pin_dir_path, x, *args, **kwargs):
        from jinja2 import Environment

        env = Environment()
        template = env.from_string(self.html_template.read_text())

        meta = super().prepare_pin_version(pin_dir_path, x, *args, **kwargs)

        # copy in files needed by index.html ----------------------------------
        crnt_files = set([meta.file] if isinstance(meta.file, str) else meta.file)
        to_add = [str(p) for p in self.html_assets_dir.rglob("*")]
        overlap = set(to_add) & crnt_files
        if overlap:
            raise PinsError(
                f"Generating an index.html would overwrite these files: {overlap}"
            )

        # recursively copy all assets into prepped pin version dir
        shutil.copytree(self.html_assets_dir, pin_dir_path, dirs_exist_ok=True)

        # render index.html ------------------------------------------------

        all_files = [meta.file] if isinstance(meta.file, str) else meta.file
        pin_files = ", ".join(f"""<a href="{x}">{x}</a>""" for x in all_files)

        context = {
            "pin_name": "TODO",
            "pin_files": pin_files,
            "pin_metadata": meta,
        }

        # data preview ----

        # TODO: move out data_preview logic? Can we draw some limits here?
        #       note that the R library uses jsonlite::toJSON

        import pandas as pd
        import json

        if isinstance(x, pd.DataFrame):
            # TODO(compat) is 100 hard-coded?
            data = json.loads(x.head(100).to_json(orient="records"))
            columns = [
                {"name": [col], "label": [col], "align": ["left"], "type": [""]}
                for col in x
            ]

            context["data_preview"] = json.dumps({"data": data, "columns": columns})
        else:
            # TODO(compat): set display none in index.html
            context["data_preview"] = json.dumps({})

        rendered = template.render(context)
        (Path(pin_dir_path) / "index.html").write_text(rendered)

        return meta
