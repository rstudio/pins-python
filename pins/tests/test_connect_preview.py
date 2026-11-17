import json
from html.parser import HTMLParser

import pandas as pd
import pytest

from pins import board_connect
from pins import data_dict as dd
from pins.errors import PinsError


class Preview(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.links = []
        self.codes = {}
        self.preview = ""
        self._code = None
        self._preview = False
        self.panels = {}
        self._panel = None
        self._sections = 0
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "section":
            if "data-pin-panel" in attrs:
                self._panel = attrs["id"]
                self.panels[self._panel] = ""
            if self._panel:
                self._sections += 1
        if tag == "a":
            self.links.append(attrs.get("href"))
        if tag == "code":
            self._code = attrs.get("id")
            self.codes[self._code] = ""
        if tag == "script" and "data-pagedtable-source" in attrs:
            self._preview = True

    def handle_endtag(self, tag):
        if tag == "section" and self._panel:
            self._sections -= 1
            if not self._sections:
                self._panel = None
        if tag == "code":
            self._code = None
        if tag == "script":
            self._preview = False

    def handle_data(self, text):
        if self._panel:
            self.panels[self._panel] += text
        if self._code:
            self.codes[self._code] += text
        if self._preview:
            self.preview += text


def test_standard_preview_preserves_data_and_escapes_content(tmp_path, monkeypatch):
    monkeypatch.setattr(dd, "_cli", lambda: pytest.fail("Unexpected CLI lookup"))
    attack = '</script><script>alert("unexpected")</script>'
    name = "owner/data&'s dataset"
    frame = pd.DataFrame({"value": [attack]})
    connect = board_connect(
        server_url="https://connect.example.com", api_key="offline", cache=None
    )
    meta = connect.prepare_pin_version(
        tmp_path,
        frame,
        name,
        type="csv",
        title="Sample & important pin",
        description="Publisher's & shared description",
    )
    html = (tmp_path / "index.html").read_text(encoding="utf-8")
    page = Preview(html)
    assert " · Data pin</title>" in html
    assert "#metadata" in page.links
    assert "Download metadata" in page.panels["metadata"]
    assert "Download metadata" not in page.panels["overview"]
    assert attack not in html
    assert "Data preview" in html and "Download metadata" in html
    assert "<details" not in html
    assert "Sample &amp; important pin" in html
    assert "Publisher&#39;s &amp; shared description" in html
    assert "data.txt" in page.links
    assert "data%26%27s%20dataset.csv" in page.links
    assert page.codes["pin-python"].endswith(f"board.pin_read({json.dumps(name)})")
    assert page.codes["pin-r"].endswith(f"pin_read(board, {json.dumps(name)})")
    assert json.loads(page.preview)["data"][0]["value"] == attack
    assert not (tmp_path / "dictionary.html").exists()
    assert (tmp_path / "pin.css").is_file() and (tmp_path / "pin.js").is_file()
    pd.testing.assert_frame_equal(pd.read_csv(tmp_path / meta.file), frame)


@pytest.mark.parametrize(
    "filename",
    [
        "index.html",
        "INDEX.HTML",
        "pin.css",
        "data.txt",
        "manifest.json",
        "pagedtable-1.1",
        "dictionary.html",
    ],
)
def test_preview_rejects_payload_collisions(tmp_path, monkeypatch, filename):
    monkeypatch.setattr(dd, "_cli", lambda: pytest.fail("Unexpected CLI lookup"))
    source = tmp_path / filename
    source.write_text("original payload")
    output = tmp_path / "bundle"
    output.mkdir()
    metadata = (
        {"data_dict": "tables: [{name: data, columns: []}]"}
        if filename == "dictionary.html"
        else None
    )
    connect = board_connect(
        server_url="https://connect.example.com", api_key="offline", cache=None
    )
    with pytest.raises(PinsError, match="would overwrite"):
        connect.prepare_pin_version(
            output,
            [source],
            "owner/file",
            type="file",
            metadata=metadata,
            object_name=[source.name],
        )
    assert source.read_text(encoding="utf-8") == "original payload"
