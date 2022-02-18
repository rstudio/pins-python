import os
import requests


class RsConnectApiError(Exception):
    pass


class RsConnectApi:
    api_key: "str | None"
    server_url: "str"
    base_v1_url: "str"

    def __init__(self, server_url, api_key=None):
        self.server_url = server_url
        self.api_key = api_key

    # utility functions -------------------------------------------------------

    @property
    def base_v1_url(self):
        return f"{self.server_url}/__api__/v1"

    @staticmethod
    def _get_params(d):
        """Helper function to return the arguments passed to a method as a dictionary.

        Note that it should be the first thing called, using this format:
        self._get_params(locals())
        """

        kwargs = {k: v for k, v in d.items() if k != "self" if v is not None}
        return kwargs

    def _get_api_key(self):
        # from manually specified
        # from env
        if self.api_key is not None:
            return self.api_key

        return os.environ["RSCONNECT_API_KEY"]

    def _get_headers(self):
        return {"Authorization": f"key {self._get_api_key()}"}

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

        return r.json()

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

    def ping(self):
        # TODO: may not be under __api__ route
        return self._raw_query(f"{self.server_url}/__ping__")

    def get_user(self):
        return self.query_v1("user")

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
            return self.walk_paginated_offsets(self.query_v1, "users", "GET")
        else:
            return self.query_v1("users", params=params)

    def get_content(self, owner_guid: str = None, name: str = None):
        params = self._get_params(locals())

        return self.query_v1("content", params=params)

    def get_content_item(self, guid):
        # endpoint = f"{self.base_v1_url}/content/{guid}"
        pass


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


class RsConnectFs:
    def __init__(self, server_url, **kwargs):
        if isinstance(server_url, RsConnectApi):
            self.api = server_url
        else:
            self.api = RsConnectApi(server_url, **kwargs)

    def ls(self, path, details=False, **kwargs):
        parts = path.split("/")
        if len(parts) > 2:
            raise ValueError(
                "path must have form {owner_guid} or {owner_guid}/{content_name}"
            )

        elif len(parts) == 1:
            if parts[0] == "":
                all_results = self.api.get_users()
            else:
                all_results = self.api.get_content(parts[0])

        elif len(parts) == 2:
            all_results = self.api.query(
                "applications/", filter="content_type:pin", count=1000
            )

        if not details:
            return [entry["username"] for entry in all_results]

        return all_results

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
