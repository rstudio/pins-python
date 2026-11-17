import base64
import hashlib
from html.parser import HTMLParser

import pandas as pd
import pytest
import yaml

from pins import board_connect, board_folder
from pins import data_dict as dd
from pins.errors import PinsError
from pins.tests.test_data_dict import DICTIONARY, _require_cli


class PolicyParser(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.policy = ""
        self.scripts = []
        self._script = None
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "meta" and attrs.get("http-equiv") == "Content-Security-Policy":
            self.policy = attrs["content"]
        if tag == "script":
            self._script = ""

    def handle_data(self, data):
        if self._script is not None:
            self._script += data

    def handle_endtag(self, tag):
        if tag == "script" and self._script is not None:
            if self._script:
                self.scripts.append(self._script)
            self._script = None


def assert_policy(html):
    page = PolicyParser(html)
    assert "script-src 'self'" in page.policy
    assert "'unsafe-inline'" not in page.policy
    assert "object-src 'none'" in page.policy and "base-uri 'none'" in page.policy
    for script in page.scripts:
        expected = base64.b64encode(hashlib.sha256(script.encode()).digest()).decode()
        assert f"'sha256-{expected}'" in page.policy
    assert "javascript:alert(1)" not in page.scripts


def test_renderer_protects_inline_scripts_without_allowing_inline_urls():
    html = '<html><head></head><body><script>const title = "é";</script></body></html>'
    protected = dd._secure_html(html, theme=True)
    assert_policy(protected)
    assert '<script src="theme.js"></script>' in protected


def test_incomplete_renderer_html_fails_and_removes_output(tmp_path, monkeypatch):
    def render(args):
        args[-1].write_text("<html><head>missing end")

    monkeypatch.setattr(dd, "_run", render)
    with pytest.raises(PinsError, match="complete head"):
        dd.render(DICTIONARY, tmp_path)
    assert not list(tmp_path.iterdir())


def test_real_cli_applies_policy_to_dictionary_prose(tmp_path):
    _require_cli()
    document = yaml.safe_load(DICTIONARY)
    attack = "<script>window.__pins_injected=1</script><img src=x onerror=alert(1)>"
    document["label"] = attack
    document["description"] = attack
    table = document["tables"][0]
    table["description"] = attack + " [click](javascript:alert%281%29)"
    column = table["columns"][0]
    column.update(type="string", label=attack, description=attack, examples=[attack])
    text = yaml.safe_dump(document)
    dd.render(text, tmp_path, filename="dictionary.html")
    html = (tmp_path / "dictionary.html").read_text(encoding="utf-8")
    assert_policy(html)
    assert "MutationObserver" in html
    assert "<script>window.__pins_injected=1</script>" not in html
    assert "<img src=x onerror=alert(1)>" not in html


def test_identical_write_does_not_draft_or_render(tmp_path, monkeypatch, capsys):
    frame = pd.DataFrame({"id": [1, 2]})
    board = board_folder(tmp_path / "board", versioned=False)
    board.pin_write(frame, "data", type="csv")
    monkeypatch.setattr(dd, "_cli", lambda: pytest.fail("Skipped write consulted CLI"))
    board.pin_write(frame, "data", type="csv", data_dict=True)
    assert "force_identical_write=True" in capsys.readouterr().err
    pd.testing.assert_frame_equal(board.pin_read("data"), frame)


def test_connect_identical_dictionary_write_avoids_render(tmp_path, monkeypatch):
    frame = pd.DataFrame({"id": [1, 2]})
    board = board_connect(server_url="https://example.com", api_key="offline", cache=None)
    old = board._create_meta(
        tmp_path, frame, "owner/data", type="csv", metadata={"data_dict": DICTIONARY}
    )
    board.__dict__["user_name"] = "owner"
    monkeypatch.setattr(board, "pin_exists", lambda name: True)
    monkeypatch.setattr(board, "pin_meta", lambda name: old)
    monkeypatch.setattr(
        dd, "render", lambda *args, **kwargs: pytest.fail("Skipped write rendered")
    )
    assert (
        board._pin_store(
            frame, "owner/data", type="csv", metadata={"data_dict": DICTIONARY}
        )
        == old
    )


def test_ordinary_writes_preserve_preparation_override(tmp_path, monkeypatch):
    board = board_folder(tmp_path / "board")
    prepare = board.prepare_pin_version
    calls = []

    def override(*args, **kwargs):
        calls.append(True)
        return prepare(*args, **kwargs)

    monkeypatch.setattr(board, "prepare_pin_version", override)
    board.pin_write(pd.DataFrame({"id": [1]}), "data", type="csv")
    assert calls == [True]
