from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import ClassVar

from fsspec import AbstractFileSystem

from ..utils import isfilelike
from .api import (
    RSC_CODE_OBJECT_DOES_NOT_EXIST,
    BaseEntity,
    Bundle,
    Content,
    RsConnectApi,
    RsConnectApiError,
    RsConnectApiMissingContentError,
    RsConnectApiRequestError,
    RsConnectApiResultError,
    User,
)

# Misc ----


def _not_impl_args_kwargs(args, kwargs):
    return NotImplementedError(
        "Additional args and kwargs not supported." f"\nArgs: {args}\nKwargs: {kwargs}"
    )


# Bundles ----


@dataclass
class PinBundleManifestMetadata:
    appmode: str = "static"
    primary_rmd: str | None = None
    primary_html: str = "index.html"
    content_category: str = "pin"
    has_parameters: bool = False


@dataclass
class PinBundleManifest:
    version: int = 1
    local: str = "en_US"
    platform: str = "3.5.1"
    metadata: PinBundleManifestMetadata = field(default_factory=PinBundleManifestMetadata)
    packages: None = None
    files: list = field(default_factory=list)
    users: None = None

    @classmethod
    def from_directory(cls, dir_name, recursive: bool = True):
        root_dir = Path(dir_name)

        paths = root_dir.rglob("*") if recursive else root_dir.glob("*")
        flat_rel_files = [str(p.relative_to(root_dir)) for p in paths]

        return cls(files=flat_rel_files)

    @classmethod
    def add_manifest_to_directory(cls, dir_name: str | Path, **kwargs) -> None:
        import json

        # TODO(question): R lib uses RSCONNECT_TAR env variable
        bundle = cls.from_directory(dir_name, **kwargs)
        with (Path(dir_name) / "manifest.json").open("w") as f_manifest:
            json.dump(bundle.to_dict(), f_manifest)

    def to_dict(self):
        return asdict(self)


# FSSPEC ----------------------------------------------------------------------


@dataclass
class EmptyPath:
    pass


@dataclass
class UserPath:
    username: str

    def path_to_field(self, field_name):
        all_fields = [field.name for field in fields(self)]
        keep_fields = all_fields[: all_fields.index(field_name) + 1]
        return "/".join(getattr(self, k) for k in keep_fields)


@dataclass
class ContentPath(UserPath):
    content: str


@dataclass
class BundlePath(ContentPath):
    bundle: str


@dataclass
class BundleFilePath(BundlePath):
    file_name: str


class RsConnectFs(AbstractFileSystem):
    protocol: ClassVar[str | tuple[str, ...]] = "rsc"

    def __init__(self, server_url, **kwargs):
        if isinstance(server_url, RsConnectApi):
            self.api = server_url
        else:
            self.api = RsConnectApi(server_url, **kwargs)

        self._user_name_cache = {}
        self._content_name_cache = {}

    def ls(self, path, details=False, **kwargs) -> Sequence[BaseEntity] | Sequence[str]:
        """List contents of Rstudio Connect Server.

        Parameters
        ----------
        path: str
            "" -> users
            "<username>" -> user content
            "<username>/<content>" -> user content bundles
        """

        if isinstance(self.parse_path(path), EmptyPath):
            # root path specified, so list users
            all_results = self.api.get_users()

        else:
            entity = self.info(path)

            if isinstance(entity, User):
                all_results = self.api.get_content(entity["guid"])
            elif isinstance(entity, Content):
                all_results = self.api.get_content_bundles(entity["guid"])
            else:
                raise ValueError(
                    "path must have form {username} or {username}/{content_name}"
                )

        if not details:
            return [entry.get_name() for entry in all_results]

        return all_results

        # all_results = self.api.query(
        #    "applications/", filter="content_type:pin", count=1000
        # )

    def put(
        self,
        lpath,
        rpath,
        recursive=False,
        *args,
        access_type="acl",
        deploy=True,
        cls_manifest=PinBundleManifest,
        **kwargs,
    ) -> None:
        """Put a bundle onto Rstudio Connect.

        Parameters
        ----------
        lpath: str
            A path to the local bundle directory.
        rpath: str
            A path to the content where the bundle is being put.
        access_type:
            Who can use and view this content? Must be one of all, logged_in or acl.
        cls_manifest:
            If maniest does not exist, a class with an .add_manifest_to_directory()
            method.

        """

        parsed = self.parse_path(rpath)

        if len(args) or len(kwargs):
            raise _not_impl_args_kwargs(args, kwargs)

        if recursive is False:
            raise NotImplementedError(
                "Must set recursive to True in order to put any RSConnect content."
            )

        if not isinstance(parsed, ContentPath):
            # TODO: replace all these with a custom PathError
            raise ValueError("Path must point to content.")

        # Create content item if missing ----

        try:
            content = self.info(rpath)
        except RsConnectApiMissingContentError:
            # TODO: this could be seen as analogous to mkdir (which gets
            # called by pins anyway)
            content = self.api.post_content_item(parsed.content, access_type)

        # Create bundle (with manifest.json inserted if missing) ----

        if not (Path(lpath) / "manifest.json").exists():
            # TODO(question): does R pins copy content to tmp directory, or
            # insert mainfest.json into the source directory?
            cls_manifest.add_manifest_to_directory(lpath)

        bundle = self.api.post_content_bundle(content["guid"], lpath)

        # Deploy bundle ----

        if deploy:
            task = self.api.post_content_item_deploy(bundle["content_guid"], bundle["id"])

            task = self.api.poll_tasks(task["task_id"])
            if task["code"] != 0 or not task["finished"]:
                raise RsConnectApiError(f"deployment failed for task: {task}")

        # TODO: should return bundle itself?
        return f"{rpath}/{bundle['id']}"

    def open(self, path: str, mode: str = "rb", *args, **kwargs):
        """Open a file inside an RStudio Connect bundle."""

        if mode != "rb":
            raise NotImplementedError()

        parsed = self.parse_path(path)

        if not isinstance(parsed, BundleFilePath):
            raise ValueError(
                "Path to a bundle file required. "
                "e.g. <user_name>/<content_name>/<bundle_id>/<file_name>"
            )

        bundle = self.info(
            parsed.path_to_field("bundle")
        )  # f"{parsed.username}/{parsed.content}/{parsed.bundle}")

        # TODO: do whatever other remote backends do
        from io import BytesIO

        f = BytesIO()

        self.api.misc_get_content_bundle_file(
            bundle["content_guid"], bundle["id"], parsed.file_name, f
        )
        f.seek(0)
        return f

    def get(self, rpath, lpath, recursive=False, *args, **kwargs) -> None:
        """Fetch a bundle or file from RStudio Connect."""
        parsed = self.parse_path(rpath)

        if recursive:
            if not isinstance(parsed, BundlePath):
                raise ValueError("Must receive path to bundle for recursive get.")

            bundle = self.info(rpath)
            self.api.get_content_bundle_archive(
                bundle["content_guid"], bundle["id"], lpath
            )

        elif isinstance(parsed, BundleFilePath):
            bundle = self.info(parsed.path_to_field("bundle"))
            self.api.misc_get_content_bundle_file(
                bundle["content_guid"], bundle["id"], parsed.file_name, lpath
            )

    def get_file(self, rpath, lpath, **kwargs):
        data = self.cat_file(rpath, **kwargs)
        if isfilelike(lpath):
            lpath.write(data)
        else:
            with open(lpath, "wb") as f:
                f.write(data)

    def exists(self, path: str, **kwargs) -> bool:
        try:
            self.info(path)
            return True
        except RsConnectApiMissingContentError:
            return False

    def mkdir(
        self, path, create_parents=True, *args, access_type="acl", **kwargs
    ) -> None:
        parsed = self.parse_path(path)

        if len(args) or len(kwargs):
            raise _not_impl_args_kwargs(args, kwargs)

        if not isinstance(parsed, ContentPath):
            raise ValueError(f"Requires path to content, but received: {path}")

        if self.exists(path):
            raise FileExistsError(path)

        # TODO: could implement and call makedirs, but seems overkill
        self.api.post_content_item(parsed.content, access_type, **kwargs)

    def info(self, path, **kwargs) -> User | Content | Bundle:
        # TODO: source of fsspec info uses self._parent to check cache?
        # S3 encodes refresh (for local cache) and version_id arguments

        return self._get_entity_from_path(path)

    def rm(self, path, recursive=False, maxdepth=None) -> None:
        parsed = self.parse_path(path)

        # guards ----
        if maxdepth is not None:
            raise NotImplementedError("rm maxdepth argument not supported.")
        if isinstance(parsed, BundleFilePath):
            raise ValueError("Cannot rm a bundle file.")

        # time to delete things ----
        entity = self.info(path)

        if isinstance(entity, User):
            raise ValueError("Cannot rm a user.")
        if isinstance(entity, Content):
            if not recursive:
                raise ValueError("Must set recursive to true if deleting content.")

            self.api.delete_content_item(entity["guid"])

        elif isinstance(entity, Bundle):
            self.api.delete_content_bundle(entity["content_guid"], entity["id"])
        else:
            raise ValueError("Cannot entity: {type(entity)}")

    # Utils ----

    def parse_path(self, path):
        # root can be indicated by a slash
        if path.startswith("/"):
            path = path[1:]

        parts = path.split("/")
        if path.strip() == "":
            return EmptyPath()
        elif len(parts) == 1:
            return UserPath(*parts)
        elif len(parts) == 2:
            return ContentPath(*parts)
        elif len(parts) == 3:
            return BundlePath(*parts)
        elif len(parts) == 4:
            return BundleFilePath(*parts)
        else:
            raise ValueError(f"Unable to parse path: {path}")

    def _get_entity_from_path(self, path):
        parsed = self.parse_path(path)

        # guard against empty paths
        if isinstance(parsed, EmptyPath):
            raise ValueError(f"Cannot fetch root path: {path}")

        # note this sequence of ifs is essentially a case statement going down
        # a line from parent -> child -> grandchild
        if isinstance(parsed, UserPath):
            crnt = user = self._get_user_from_name(parsed.username)

        if isinstance(parsed, ContentPath):
            user_guid = user["guid"]

            # user_guid + content name should uniquely identify content, but
            # double check to be safe.
            crnt = content = self._get_content_from_name(user_guid, parsed.content)

        if isinstance(parsed, BundlePath):
            content_guid = content["guid"]
            crnt = self._get_content_bundle(content_guid, parsed.bundle)

        return crnt

    def _get_content_from_name(self, user_guid, content_name):
        """Fetch a single content entity."""

        # user_guid + content name should uniquely identify content, but
        # double check to be safe.
        contents = self.api.get_content(user_guid, content_name)
        if len(contents) != 1:
            err = (
                RsConnectApiMissingContentError
                if len(contents) == 0
                else RsConnectApiResultError
            )
            raise err(f"Expecting 1 content entry, but found {len(contents)}: {contents}")

        res = contents[0]

        self._content_name_cache[(user_guid, content_name)] = res["guid"]
        return res

    def _get_content_bundle(self, content_guid, bundle_id):
        """Fetch a content bundle."""

        try:
            bundle = self.api.get_content_bundle(content_guid, bundle_id)
        except RsConnectApiRequestError as e:
            if e.args[0]["code"] == RSC_CODE_OBJECT_DOES_NOT_EXIST:
                raise RsConnectApiMissingContentError(
                    f"No bundle {bundle_id} for content {content_guid}"
                )
            raise e

        return bundle

    def _get_user_from_name(self, name):
        """Fetch a single user entity from user name."""
        users = self.api.get_users(prefix=name)
        try:
            user = next(iter([x for x in users if x["username"] == name]))

            self._user_name_cache[user["username"]] = user["guid"]

            return user
        except StopIteration:
            raise ValueError(f"No user named {name} found.")
