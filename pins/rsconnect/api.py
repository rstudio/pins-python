import logging
import os
import requests
import tempfile
import json

from dataclasses import dataclass
from pathlib import Path
from functools import partial
from io import IOBase
from urllib.parse import urlencode

from collections.abc import Mapping
from typing import Sequence, TypeVar, Generic


RSC_API_KEY = "CONNECT_API_KEY"
RSC_CODE_OBJECT_DOES_NOT_EXIST = 4
RSC_CODE_INVALID_NUMERIC_PATH = 3

_log = logging.getLogger(__name__)


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

    def __init__(
        self,
        server_url: "str | None",
        api_key: "str | None" = None,
        session: "requests.Session | None" = None,
    ):
        self.server_url = server_url
        self.api_key = api_key
        self.session = requests.Session() if session is None else session

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

        return os.environ.get(RSC_API_KEY)

    def _get_headers(self):
        api_key = self._get_api_key()
        rsc_xsrf = self.session.cookies.get("RSC-XSRF")

        d_key = {"Authorization": f"key {api_key}"} if api_key is not None else {}
        d_rsc = {"X-RSC-XSRF": rsc_xsrf} if rsc_xsrf is not None else {}

        return {**d_key, **d_rsc}

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

        _log.debug(f"RSConnect API {method}: {url} -- {kwargs}")
        r = self.session.request(method, url, headers=headers, **kwargs)

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
            page_kwargs = {"page_number": data["current_page"] + 1}
            new_params = {**params, **page_kwargs}
            data = f_query(endpoint, method, params=new_params)

            all_results.extend(data["results"])

        return all_results

    # endpoints ---------------------------------------------------------------

    # users ----

    def get_user(self, guid: str = None) -> User:
        if guid is None:
            return User(self.query_v1("user"))

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
        self, name, access_type: str, title: str = "", description: str = "", **kwargs
    ) -> Content:
        data = self._get_params(locals(), exclude={"kwargs"})
        result = self.query_v1("content", "POST", json={**data, **kwargs})

        return Content(result)

    def post_content_item_deploy(self, guid: str, bundle_id: "str | None" = None):
        json = {"bundle_id": bundle_id} if bundle_id is not None else {}
        return self.query_v1(f"content/{guid}/deploy", "POST", json=json)

    def patch_content_item(self, guid, **kwargs) -> Content:
        """Update a content item (e.g. its title or description).

        See post_content_item method for possible arguments.
        """

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

            with tempfile.TemporaryDirectory() as tmp_dir:
                p_archive = Path(tmp_dir) / "bundle.tar.gz"

                with tarfile.open(p_archive, mode="w:gz") as tar:
                    tar.add(str(p.absolute()), arcname="")

                with open(p_archive, "rb") as f:
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
        self, filter: str, count: int = 1000, search: str = None
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
# TODO: could just move these methods into RsConnectApi?
class _HackyConnect(RsConnectApi):
    """Handles logging in to connect, rather than using an API key.

    This class allows you to create users and generate API keys on a fresh
    RStudio Connect service.
    """

    def login(self, user, password):
        res = self.query(
            "__login__",
            "POST",
            return_request=True,
            json={"username": user, "password": password},
        )
        return res

    def create_first_admin(self, user, password, email, keyname="first-key"):
        self.login(user, password)

        self.query("me")

        api_key = self.query("keys", "POST", json=dict(name=keyname))

        return RsConnectApi(self.server_url, api_key=api_key["key"])
