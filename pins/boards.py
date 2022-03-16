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

    def mkdir(self, path, create_parents=True, **kwargs) -> None:
        ...

    def rm(self, path, recursive=False, maxdepth=None) -> None:
        ...

    def info(self, path):
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
            Pin name.
        """

        return self.fs.exists(self.path_to_pin(name))

    def pin_versions(self, name: str, as_df: bool = True) -> Sequence[VersionRaw]:
        """Return available versions of a pin.

        Parameters
        ----------
        name:
            Pin name.

        """
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

    def pin_meta(self, name, version: str = None) -> Meta:
        """Return metadata about a pin.

        Parameters
        ----------
        name:
            Pin name.
        version: optional
            A specific pin version to retrieve.

        See Also
        --------
        BaseBoard.pin_versions

        """

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
        """List names of all pins in a board.

        Notes
        -----
        This is a low-level function; use pin_search() to get more data about
        each pin in a convenient form.
        """
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
        """Return the data stored in a pin.

        Parameters
        ----------
        name:
            Pin name.
        version:
            A specific pin version to retrieve.
        hash:
            A hash used to validate the retrieved pin data. If specified, it is
            compared against the ``pin_hash`` field retrived by ``pin_meta()``.

        """
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
    ) -> Meta:
        """Write a pin object to the board.

        Parameters
        ----------
        x:
            An object (e.g. a pandas DataFrame) to pin.
        name:
            Pin name.
        type:
            File type used to save ``x`` to disk.
        title:
            A title for the pin; most important for shared boards so that others
            can understand what the pin contains. If omitted, a brief description
            of the contents will be automatically generated.
        description:
            A detailed description of the pin contents.
        metadata:
            A dictionary containing additional metadata to store with the pin.
            This gets stored on the Meta.user field.
        versioned:
            Whether the pin should be versioned.
        created:
            A date to store in the Meta.created field. This field may be used as
            part of the pin version name.
        """
        # TODO(docs): describe options for type argument
        # TODO(docs): elaborate on default behavior for versioned parameter
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

            res = self.fs.put(tmp_dir, dst_version_path, recursive=True)

        if dst_version_path == dst_pin_path:
            # TODO(refactor): this is a RSConnect specific hack
            # since we don't know the bundle id ahead of time, the meta version
            # object is incorrect. Could fix through the meta_factory
            bundle_version = VersionRaw(res.split("/")[-1])
            meta.version = bundle_version

        return meta

    def pin_download(self, name, version=None, hash=None):
        """TODO: Download the files contained in a pin.

        This method only downloads the files in a pin. In order to read and load
        pin data as an object (e.g. a pandas DataFrame), use ``pin_read()``.

        Parameters
        ----------
        name:
            Pin name.
        version:
            A specific pin version to retrieve.
        hash:
            A hash used to validate the retrieved pin data. If specified, it is
            compared against the ``pin_hash`` field retrived by ``pin_meta()``.


        """
        raise NotImplementedError()

    def pin_upload(self, paths, name=None, title=None, description=None, metadata=None):
        """TODO: Write a pin based on paths to one or more files.

        This method simply uploads the files given, so they can be downloaded later
        using ``pin_download()``.
        """
        # TODO(question): why does this method exist? Isn't it equiv to a user
        # doing this?: pin_write(board, c("filea.txt", "fileb.txt"), type="file")
        # pin_download makes since, because it will download *regardless of type*
        raise NotImplementedError()

    def pin_version_delete(self, name: str, version: str):
        """Delete a single version of a pin.

        Parameters
        ----------
        name:
            Pin name.
        version:
            Version identifier.
        """

        pin_name = self.path_to_pin(name)

        pin_version_path = self.construct_path([pin_name, version])
        self.fs.rm(pin_version_path, recursive=True)

    def pin_versions_prune(self, name, n=None, days=None):
        """TODO: Delete old versions of a pin.

        Parameters
        ----------
        name:
            Pin name.
        n, days:
            Pick one of n or days to choose how many versions to keep. n = 3 will
            keep the last three versions, days = 14 will keep all the versions in
            the last 14 days.

        Notes
        -----
        Regardless of what values you set, pin_versions_prune() will never delete
        the most recent version.

        """
        raise NotImplementedError()

    def pin_search(self, search=None):
        """TODO: Search for pins.

        The underlying search method depends on the board implementation, but most
        will search for text in the pin name and title.

        Parameters
        ----------
        search:
            A string to search for. By default returns all pins.

        """
        raise NotImplementedError()

    def pin_delete(self, names: "str | Sequence[str]"):
        """TODO: Delete a pin (or pins), removing it from the board.

        Parameters
        ----------
        names:
            The names of one or more pins to delete.
        """

        if isinstance(names, str):
            names = [names]

        for name in names:
            if not self.pin_exists(name):
                raise PinsError("Cannot delete pin, since pin %s does not exist" % name)

            pin_name = self.path_to_pin(name)
            self.fs.rm(pin_name, recursive=True)

    def pin_browse(self, name, version=None, local=False):
        """TODO: Navigate to the home of a pin, either on the internet or locally.

        Parameters
        ----------
        name:
            Pin name.
        version:
            A specific pin version to retrieve.
        local:
            Whether to open the local copy of the pin. Defaults to showing you
            the home of the pin on the internet.

        See Also
        --------
        BaseBoard.pin_versions

        """

        raise NotImplementedError()

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
    html_template: Path = files("pins") / "rsconnect/html/index.html"

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
