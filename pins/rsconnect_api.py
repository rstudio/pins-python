import os
import requests
import tempfile
import json

from dataclasses import dataclass, asdict, field, fields
from pathlib import Path
from functools import partial
from io import IOBase
from urllib.parse import urlencode

from collections.abc import Mapping
from typing import Sequence, TypeVar, Generic


RSC_CODE_OBJECT_DOES_NOT_EXIST = 4


def _download_file(response, local_fname):
    """Download a potentially large file. Note that this mutates the response.

    Adapted from https://stackoverflow.com/a/39217788
    """

    import shutil

    response.raw.read = partial(response.raw.read, decode_content=True)

    if isinstance(local_fname, IOBase):
        shutil.copyfileobj(response.raw, local_fname)
    else:
        with open(local_fname, "wb") as f:
            shutil.copyfileobj(response.raw, f)


# Exceptions ------------------------------------------------------------------


class RsConnectApiError(Exception):
    pass


class RsConnectApiRequestError(RsConnectApiError):
    pass


class RsConnectApiResultError(RsConnectApiError):
    pass


class RsConnectApiMissingContentError(RsConnectApiError):
    pass


# Data ------------------------------------------------------------------------


class BaseEntity(Mapping):
    def __init__(self, d: Mapping):
        self._d = d

    def __getitem__(self, k):
        return self._d[k]

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def __repr__(self):
        repr_content = repr(self._d)
        return f"{self.__class__.__name__}({repr_content})"

    def _repr_pretty_(self, p, cycle=False):
        p.text(f"{self.__class__.__name__}(")
        p.pretty(self._d)
        p.text(")")


class User(BaseEntity):
    def get_id(self) -> str:
        return self._d["guid"]

    def get_name(self) -> str:
        return self._d["username"]


class Content(BaseEntity):
    def get_id(self) -> str:
        return self._d["guid"]

    def get_name(self) -> str:
        return self._d["name"]


class Bundle(BaseEntity):
    def get_id(self) -> str:
        return self._d["id"]

    def get_name(self) -> str:
        return self._d["id"]


class Task(BaseEntity):
    def get_id(self) -> str:
        return self._d["id"]

    def get_name(self) -> str:
        return self._d["id"]


# Pagination result container ----

T = TypeVar("T")


@dataclass
class Paginated(Generic[T]):
    def __init__(self, results: T, cursor: Mapping):
        # holding off on defining cursor structure, since
        # it seems like there are multiple types of cursors
        self.results = results
        self.cursor = cursor


# API -------------------------------------------------------------------------


class RsConnectApi:
    api_key: "str | None"
    server_url: "str"

    def __init__(self, server_url, api_key=None):
        self.server_url = server_url
        self.api_key = api_key

    # utility functions -------------------------------------------------------

    @property
    def base_v1_url(self) -> str:
        return f"{self.server_url}/__api__/v1"

    @staticmethod
    def _get_params(d, exclude: set = None):
        """Helper function to return the arguments passed to a method as a dictionary.

        Note that it should be the first thing called, using this format:
        self._get_params(locals())
        """

        if exclude is None:
            full_exclude = {"self"}
        else:
            full_exclude = {"self"} | set(exclude)

        kwargs = {k: v for k, v in d.items() if k not in full_exclude if v is not None}
        return kwargs

    def _get_api_key(self):
        # from manually specified
        # from env
        if self.api_key is not None:
            return self.api_key

        return os.environ["RSCONNECT_API_KEY"]

    def _get_headers(self):
        return {"Authorization": f"key {self._get_api_key()}"}

    def _validate_json_response(self, data: "dict | list"):
        if isinstance(data, list):
            return

        # note that code is 0 for successful tasks
        code = data.get("code")
        if code is not None and code != 0:
            raise RsConnectApiRequestError(data)

    def _validate_delete_response(self, r):
        try:
            # if we get json back it should always be an api error
            data = r.json()
            self._validate_json_response(data)

            # this should never be triggered
            raise ValueError(
                "Unknown json returned by delete_content endpoint: %s" % data
            )
        except json.JSONDecodeError:
            # fallback to at least raising status errors
            r.raise_for_status()

    def query_v1(self, route, method="GET", return_request=False, **kwargs):
        endpoint = f"{self.base_v1_url}/{route}"

        return self._raw_query(endpoint, method, return_request, **kwargs)

    def query(self, route, method="GET", return_request=False, **kwargs):
        endpoint = f"{self.server_url}/__api__/{route}"

        return self._raw_query(endpoint, method, return_request, **kwargs)

    def _raw_query(self, url, method="GET", return_request=False, **kwargs):
        if "headers" in kwargs:
            raise KeyError("cannot specify headers param in kwargs")

        headers = self._get_headers()

        r = requests.request(method, url, headers=headers, **kwargs)

        if return_request:
            return r
        else:
            # the API can return http error codes AND codes in a json payload.
            # to handle this, we prefer the json error codes, but fallback to
            # checking for an HTTP error is no valid json was given.
            try:
                data = r.json()
                self._validate_json_response(data)
                return data
            except json.JSONDecodeError:
                r.raise_for_status()

    def walk_paginated_offsets(self, f_query, endpoint, method, params=None, **kwargs):
        if params is None:
            params = {}

        all_results = []
        data = f_query(endpoint, method, params=params)

        all_results.extend(data["results"])

        while data["results"]:
            print("FETCHING")
            page_kwargs = {"page_number": data["current_page"] + 1}
            new_params = {**params, **page_kwargs}
            data = f_query(endpoint, method, params=new_params)

            all_results.extend(data["results"])
            print(data["results"])

        return all_results

    # endpoints ---------------------------------------------------------------

    # users ----

    def get_user(self, guid: str = None) -> User:
        if guid is None:
            return self.query_v1("user")

        result = self.query_v1(f"user/{guid}")
        return User(result)

    def get_users(
        self,
        prefix: "str | None" = None,
        user_role: "str | None" = None,
        account_status: "str | None" = None,
        page_number: "int | None" = None,
        page_size: "int | None" = None,
        asc_order: "bool | None" = None,
        walk_pages=True,
    ) -> "Sequence[User] | Sequence[dict]":

        params = {k: v for k, v in locals().items() if k != "self" if v is not None}

        if walk_pages:
            result = self.walk_paginated_offsets(self.query_v1, "users", "GET", params)
            return [User(d) for d in result]
        else:
            result = self.query_v1("users", params=params)
            return result

    # content ----

    def get_content(
        self, owner_guid: str = None, name: str = None
    ) -> Sequence[Content]:
        params = self._get_params(locals())

        results = self.query_v1("content", params=params)
        return [Content(d) for d in results]

    def get_content_item(self, guid: str) -> Content:
        result = self.query_v1(f"content/{guid}")
        return Content(result)

    def post_content_item(
        self, name, access_type, title: str = "", description: str = "", **kwargs
    ) -> Content:
        data = self._get_params(locals(), exclude={"kwargs"})
        result = self.query_v1("content", "POST", json={**data, **kwargs})

        return Content(result)

    def post_content_item_deploy(self, guid: str, bundle_id: "str | None" = None):
        json = {"bundle_id": bundle_id} if bundle_id is not None else {}
        return self.query_v1(f"content/{guid}/deploy", "POST", json=json)

    def patch_content_item(self, guid, **kwargs) -> Content:
        # see https://docs.rstudio.com/connect/api/#patch-/v1/content/{guid}
        result = self.query_v1(f"content/{guid}", "PATCH", json=kwargs)

        return Content(result)

    def delete_content_item(self, guid: str) -> None:
        """Delete content.

        Note that this method returns None if successful. Otherwise, it raises an error.
        """

        # if deletion is sucessful, then it will return an empty body, so we
        # need to check the response manually.
        r = self.query_v1(f"content/{guid}", "DELETE", return_request=True)

        self._validate_delete_response(r)

    def delete_content_bundle(self, guid: str, id: str) -> None:
        r = self.query_v1(f"content/{guid}/bundles/{id}", "DELETE", return_request=True)

        self._validate_delete_response(r)

    # bundles ----

    def get_content_bundles(self, guid: str) -> Sequence[Bundle]:
        result = self.query_v1(f"content/{guid}/bundles")
        return [Bundle(d) for d in result]

    def get_content_bundle(self, guid: str, id: int) -> Bundle:
        result = self.query_v1(f"content/{guid}/bundles/{id}")
        return Bundle(result)

    def get_content_bundle_archive(
        self, guid: str, id: str, f_obj: "str | IOBase"
    ) -> None:

        r = self.query_v1(
            f"content/{guid}/bundles/{id}/download", stream=True, return_request=True
        )
        r.raise_for_status()
        _download_file(r, f_obj)

    def post_content_bundle(self, guid, fname, gzip=True) -> Bundle:
        route = f"content/{guid}/bundles"
        f_request = partial(self.query_v1, route, "POST")

        p = Path(fname)
        if p.is_dir() and gzip:
            import tarfile

            with tempfile.NamedTemporaryFile(mode="wb", suffix=".tar.gz") as tmp:
                with tarfile.open(fileobj=tmp.file, mode="w:gz") as tar:
                    tar.add(str(p.absolute()), arcname="")

                # close the underlying file. note we don't call the top-level
                # close method, since that would delete the temporary file
                tmp.file.close()

                with open(tmp.name, "rb") as f:
                    result = f_request(data=f)
        else:
            with open(str(p.absolute()), "rb") as f:
                result = f_request(data=f)

        return Bundle(result)

    # tasks ----

    def get_tasks(self, id: str, first: int = None, wait: int = None) -> Task:
        params = self._get_params(locals())
        del params["id"]

        return self.query_v1(f"tasks/{id}", params=params)

    def poll_tasks(self, id: str, first: int = None, wait: int = 1) -> Task:
        """Poll a task until complete."""

        json = self.get_tasks(id, first, wait)
        while not json["finished"]:
            json = self.get_tasks(id, json["last"], wait)

        return json

    # non-api endpointsmisc ----

    def misc_ping(self):
        return self._raw_query(f"{self.server_url}/__ping__")

    def misc_get_content_bundle_file(
        self, guid: str, id: str, fname: str, f_obj: "str | IOBase | None" = None
    ):
        if f_obj is None:
            f_obj = fname

        route = f"{self.server_url}/content/{guid}/_rev{id}/{fname}"

        r = self._raw_query(route, return_request=True, stream=True)
        r.raise_for_status()

        _download_file(r, f_obj)

    def misc_get_applications(
        self, filter: str, count: int = 1000
    ) -> Paginated[Sequence[Content]]:
        # TODO(question): R pins does not handle pagination, but we could do it here.
        #       Note that R pins also just gets first 1000 entries
        raw_params = self._get_params(locals())
        params = urlencode(raw_params, safe=":+")
        result = self.query("applications", params=params)
        return Paginated(
            list(map(Content, result["applications"])),
            {k: v for k, v in result.items() if k != "applications"},
        )


# ported from github.com/rstudio/connectapi
class _HackyConnect(RsConnectApi):
    """Handles logging in to connect, rather than using an API key.

    This class allows you to create users and generate API keys on a fresh
    RStudio Connect service.
    """

    xsrf: "None | str"

    def __init__(self, *args, **kwargs):
        self.xsrf = None
        super().__init__(*args, **kwargs)

    def _get_headers(self):
        return {"X-RSC-XSRF": self.xsrf}

    def login(self, user, password):
        res = self.query(
            "__login__",
            "POST",
            return_request=True,
            json={"username": user, "password": password},
        )
        self.xsrf = res.cookies["RSC-XSRF"]
        return res

    def create_first_admin(self, user, password, email, keyname="first-key"):
        # TODO(question): this is run in the R rsconnect, but it returns json
        # error codes. tests run okay without it...
        # self.query_v1(
        #     "users", "POST", json=dict(username=user, password=password, email=email)
        #
        # )

        res = self.login(user, password)

        self.query("me", cookies=res.cookies)

        api_key = self.query(
            "keys", "POST", json=dict(name=keyname), cookies=res.cookies
        )

        return RsConnectApi(self.server_url, api_key=api_key["key"])


@dataclass
class PinBundleManifestMetadata:
    appmode: str = "static"
    primary_rmd: "str|None" = None
    primary_html: str = "index.html"
    content_category: str = "pin"
    has_parameters: bool = False


@dataclass
class PinBundleManifest:
    version: int = 1
    local: str = "en_US"
    platform: str = "3.5.1"
    metadata: PinBundleManifestMetadata = field(
        default_factory=PinBundleManifestMetadata
    )
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
    def add_manifest_to_directory(cls, dir_name: "str | Path", **kwargs) -> None:
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


class RsConnectFs:
    def __init__(self, server_url, **kwargs):
        if isinstance(server_url, RsConnectApi):
            self.api = server_url
        else:
            self.api = RsConnectApi(server_url, **kwargs)

    def ls(
        self, path, details=False, **kwargs
    ) -> "Sequence[BaseEntity] | Sequence[str]":
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
        cls_manifest:
            If maniest does not exist, a class with an .add_manifest_to_directory()
            method.

        """

        parsed = self.parse_path(rpath)

        if recursive is False:
            raise NotImplementedError(
                "Must set recursive to True in order to put any RSConnect content."
            )

        if not isinstance(parsed, ContentPath):
            # TODO: replace all these with a custom PathError
            raise ValueError("Path must point to content.")

        try:
            content = self.info(rpath)
        except RsConnectApiMissingContentError:
            # TODO: this could be seen as analogous to mkdir (which gets
            # called by pins anyway)
            # TODO: hard-coded acl bad?
            content = self.api.post_content_item(parsed.content, "acl")

        bundle = self.api.post_content_bundle(content["guid"], lpath)

        if deploy:
            task = self.api.post_content_item_deploy(
                bundle["content_guid"], bundle["id"]
            )

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

    def exists(self, path: str, **kwargs) -> bool:
        try:
            self.info(path)
            return True
        except RsConnectApiMissingContentError:
            return False

    def mkdir(self, path, create_parents=True, **kwargs) -> None:
        parsed = self.parse_path(path)

        if not isinstance(parsed, ContentPath):
            raise ValueError(f"Requires path to content, but received: {path}")

        if self.exists(path):
            raise FileExistsError(path)

        # TODO: could implement and call makedirs, but seems overkill
        # TODO: hard-coded "acl"?
        self.api.post_content_item(parsed.content, "acl", **kwargs)

    def info(self, path, **kwargs) -> "User | Content | Bundle":
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
            raise err(
                f"Expecting 1 content entry, but found {len(contents)}: {contents}"
            )
        return contents[0]

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
            user_guid = next(iter([x for x in users if x["username"] == name]))
            return user_guid
        except StopIteration:
            raise ValueError(f"No user named {name} found.")
