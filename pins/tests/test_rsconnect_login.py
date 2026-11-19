import json

import pytest
import requests

from pins.rsconnect.api import LoginConnectApi, RsConnectApiRequestError


@pytest.mark.parametrize(
    "key_status,legacy_status,json_error",
    [
        (200, None, False),
        (200, None, True),
        (404, 200, False),
        (404, 200, True),
        (403, None, False),
        (403, None, True),
        (500, None, False),
        (404, 403, False),
    ],
)
def test_login_create_api_key(monkeypatch, key_status, legacy_status, json_error):
    calls = []
    responses = [
        (200, {}),
        (200, {"guid": "test-user"}),
        (
            key_status,
            {"key": "modern-key"} if key_status == 200 and not json_error else None,
        ),
    ]
    if legacy_status is not None:
        responses.append(
            (legacy_status, {"key": "legacy-key"} if legacy_status == 200 else None)
        )

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        status, payload = responses.pop(0)
        if payload is None and json_error:
            payload = {"code": 3, "error": "request failed"}
        response = requests.Response()
        response.status_code = status
        response.url = url
        response._content = (
            json.dumps(payload).encode() if payload is not None else b"request failed"
        )
        return response

    session = requests.Session()
    monkeypatch.setattr(session, "request", request)
    client = LoginConnectApi("user", "password", "http://connect.test", session=session)

    error_status = legacy_status if key_status == 404 else key_status
    if key_status == 200 and json_error:
        with pytest.raises(RsConnectApiRequestError):
            client.create_api_key("test-key")
    elif error_status == 200:
        assert client.create_api_key("test-key") == (
            "legacy-key" if legacy_status is not None else "modern-key"
        )
    else:
        with pytest.raises(requests.HTTPError) as error:
            client.create_api_key("test-key")
        assert error.value.response.status_code == error_status

    expected_routes = [
        ("POST", "__login__"),
        ("GET", "v1/user"),
        ("POST", "v1/users/test-user/keys"),
    ]
    if legacy_status is not None:
        expected_routes.append(("POST", "keys"))
    assert [(method, url) for method, url, _ in calls] == [
        (method, f"http://connect.test/__api__/{route}")
        for method, route in expected_routes
    ]
    for _, _, kwargs in calls[2:]:
        assert kwargs["json"] == {"name": "test-key"}
        assert "Authorization" not in kwargs["headers"]
    assert not responses
