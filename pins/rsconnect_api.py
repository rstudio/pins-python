import os
import requests
import tempfile
import json

from dataclasses import dataclass, asdict, field
from pathlib import Path
from functools import partial
from io import IOBase


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


class RsConnectApiError(Exception):
    pass


class RsConnectApiRequestError(RsConnectApiError):
    pass


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

    def get_user(self, guid: str = None):
        if guid is None:
            return self.query_v1("user")

        return self.query_v1(f"user/{guid}")

    def get_users(
        self,
        prefix: "str | None" = None,
        user_role: "str | None" = None,
        account_status: "str | None" = None,
        page_number: "int | None" = None,
        page_size: "int | None" = None,
        asc_order: "bool | None" = None,
        walk_pages=True,
    ):

        params = {k: v for k, v in locals().items() if k != "self" if v is not None}

        if walk_pages:
            return self.walk_paginated_offsets(self.query_v1, "users", "GET", params)
        else:
            return self.query_v1("users", params=params)

    # content ----

    def get_content(self, owner_guid: str = None, name: str = None):
        params = self._get_params(locals())

        return self.query_v1("content", params=params)

    def get_content_item(self, guid: str):
        return self.query_v1(f"content/{guid}")

    def post_content_item(
        self, name, access_type, title: str = "", description: str = "", **kwargs
    ):
        data = self._get_params(locals(), exclude={"kwargs"})
        result = self.query_v1("content", "POST", json={**data, **kwargs})

        return result

    def post_content_item_deploy(self, guid: str, bundle_id: "str | None" = None):
        json = {"bundle_id": bundle_id} if bundle_id is not None else {}
        return self.query_v1(f"content/{guid}/deploy", "POST", json=json)

    def patch_content_item(self, guid, **kwargs):
        # see https://docs.rstudio.com/connect/api/#patch-/v1/content/{guid}
        result = self.query_v1(f"content/{guid}", "PATCH", json=kwargs)

        return result

    def delete_content(self, guid: str):
        """Delete content.

        Note that this method returns None if successful. Otherwise, it raises an error.
        """

        # if deletion is sucessful, then it will return an empty body, so we
        # need to check the response manually.
        r = self.query_v1(f"content/{guid}", "DELETE", return_request=True)

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

    # bundles ----

    def get_content_bundles(self, guid: str):
        return self.query_v1(f"content/{guid}/bundles")

    def get_content_bundle(self, guid: str, id: int):
        # TODO(question): could combine with get_content_bundles above?
        return self.query_v1(f"content/{guid}/bundles/{id}")

    def get_content_bundle_archive(self, guid: str, id: str, f_obj: "str | IOBase"):

        r = self.query_v1(
            f"content/{guid}/bundles/{id}/download", stream=True, return_request=True
        )
        r.raise_for_status()
        _download_file(r, f_obj)

    def post_content_bundle(self, guid, fname, gzip=True):
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
                    return f_request(data=f)
        else:
            with open(str(p.absolute()), "rb") as f:
                return f_request(data=f)

    # tasks ----

    def get_tasks(self, id: str, first: int = None, wait: int = None):
        params = self._get_params(locals())
        del params["id"]

        return self.query_v1(f"tasks/{id}", params=params)

    def poll_tasks(self, id: str, first: int = None, wait: int = 1):
        """Poll a task until complete."""

        json = self.get_tasks(id, first, wait)
        while not json["finished"]:
            json = self.get_tasks(id, json["last"], wait)

        return json

    # non-api endpointsmisc ----

    def misc_ping(self):
        # TODO: may not be under __api__ route
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

    def misc_get_applications(self, filter: str, count: int = 1000):
        params = self._get_params()
        return self._raw_query(f"{self.server_url}/applications", params=params)


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
        # TODO: should return json? What is the result anyway?
        return res

    def create_first_admin(self, user, password, email, keyname="first-key"):
        self.query_v1(
            "users", "POST", json=dict(username=user, password=password, email=email)
        )

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


class RsConnectFs:
    def __init__(self, server_url, **kwargs):
        if isinstance(server_url, RsConnectApi):
            self.api = server_url
        else:
            self.api = RsConnectApi(server_url, **kwargs)

    def ls(self, path, details=False, **kwargs):
        """List contents of Rstudio Connect Server.

        Parameters
        ----------
        path: str
            "" -> users
            "<username>" -> user content
            "<username>/<content>" -> user content bundles
        """
        parts = path.split("/")

        # Case 1: list users  ---
        if path.strip() == "":
            name_field = "username"
            all_results = self.api.get_users()

        # Case 2: list a user's content ---
        elif len(parts) == 1:
            name_field = "name"
            name = parts[0]

            # convert username -> guid and fetch content
            user_guid = self._get_user_id_from_name(name)
            all_results = self.api.get_content(user_guid)

        # Case 3: list a user content's bundles ---
        elif len(parts) == 2:
            name_field = "id"
            name, content_name = parts

            user_guid = self._get_user_id_from_name(name)

            # user_guid + content name should uniquely identify content, but
            # double check to be safe.
            content = self.api.get_content(user_guid, content_name)
            if len(content) > 1:
                raise ValueError(
                    f"Expecting 1 content entry, but found {len(content)}: {content}"
                )

            content_guid = content[0]["guid"]

            all_results = self.api.get_content_bundles(content_guid)

        if len(parts) > 3:
            raise ValueError(
                "path must have form {owner_guid} or {owner_guid}/{content_name}"
            )

        if not details:
            return [entry[name_field] for entry in all_results]

        return all_results

        # all_results = self.api.query(
        #    "applications/", filter="content_type:pin", count=1000
        # )

    def put(self, *args, **kwargs) -> None:
        pass

    def open(self, path: str, mode: str, *args, **kwargs):
        pass

    def get(self) -> None:
        pass

    def exists(self, path: str, **kwargs) -> bool:
        pass

    def mkdirs(self, *args, **kwargs) -> None:
        pass

    def _get_user_id_from_name(self, name):
        users = self.api.get_users(prefix=name)
        try:
            user_guid = next(iter([x["guid"] for x in users if x["username"] == name]))
            return user_guid
        except StopIteration:
            raise ValueError(f"No user named {name} found.")
