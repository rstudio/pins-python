import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
import yaml

from pins import board_connect, board_folder
from pins import data_dict as dd
from pins.errors import PinsError

DICTIONARY = "$version: 0.1.0\ntables:\n  - name: data\n    columns:\n      - name: id\n        type: integer\n"
MULTI_TABLE_DICTIONARY = DICTIONARY + "  - name: other\n    columns: []\n"


def _require_cli():
    try:
        dd._cli()
    except PinsError:
        if os.getenv("PINS_REQUIRE_DATA_DICT_CLI") == "1":
            raise
        pytest.skip("Optional data-dict CLI not installed")


def test_authored_metadata_roundtrip_without_cli(tmp_path, monkeypatch):
    monkeypatch.setattr(dd, "_cli", lambda: pytest.fail("Unexpected CLI lookup"))
    path = tmp_path / "dictionary.yaml"
    path.write_text(DICTIONARY)
    metadata = {"owner": "example"}
    board = board_folder(tmp_path / "board")
    frame = pd.DataFrame({"id": [1, 2]})
    board.pin_write(frame, "data", type="csv", metadata=metadata, data_dict=path)
    assert metadata == {"owner": "example"}
    assert board.pin_meta("data").user == {"owner": "example", "data_dict": DICTIONARY}
    pd.testing.assert_frame_equal(board.pin_read("data"), frame)
    # Metadata-only edits preserve existing hash semantics unless explicitly forced.
    board.pin_write(
        frame, "data", type="csv", metadata={"data_dict": DICTIONARY + "# edit\n"}
    )
    assert board.pin_meta("data").user["data_dict"] == DICTIONARY
    board.pin_write(
        frame,
        "data",
        type="csv",
        metadata={"data_dict": DICTIONARY + "# edit\n"},
        force_identical_write=True,
        created=datetime.now(timezone.utc) + timedelta(days=1),
    )
    assert board.pin_meta("data").user["data_dict"] == DICTIONARY + "# edit\n"


@pytest.mark.parametrize("dictionary", [False, 42])
def test_invalid_option(dictionary):
    with pytest.raises(PinsError, match="must be None, True"):
        dd.prepare_metadata(None, dictionary, None)


@pytest.mark.parametrize(
    "text",
    [
        "tables: [oops]",
        "tables: []",
        "!!python/object:bad {}",
        "[broken",
        "- not a mapping",
    ],
)
def test_invalid_yaml(text):
    with pytest.raises(PinsError):
        dd.prepare_metadata({"data_dict": text}, None, None)


def test_conflict_and_source_aliases():
    with pytest.raises(PinsError, match="not both"):
        dd.prepare_metadata({"data_dict": DICTIONARY}, True, None)
    text = "template: &table {name: data, source: {parquet: /secret/data.parquet}}\ntables: [*table]"
    with pytest.raises(PinsError, match="omit table source"):
        dd.prepare_metadata({"data_dict": text}, None, None)


def test_draft_and_connect_entrypoint(tmp_path):
    _require_cli()
    pytest.importorskip("pyarrow")
    frame = pd.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})
    local = board_folder(tmp_path / "board")
    meta = local.pin_write(frame, "data", type="csv", data_dict=True)
    document = yaml.safe_load(meta.user["data_dict"])
    assert [c["name"] for c in document["tables"][0]["columns"]] == ["id", "value"]
    assert "source" not in document["tables"][0]
    pd.testing.assert_frame_equal(local.pin_read("data"), frame)
    connect = board_connect(
        server_url="https://example.com", api_key="offline", cache=None
    )
    output = tmp_path / "bundle"
    output.mkdir()
    prepared = connect.prepare_pin_version(
        output, frame, "owner/data", type="csv", metadata=meta.user
    )
    assert "Data dictionary" in (output / "index.html").read_text()
    assert yaml.safe_load((output / "data.txt").read_text())["user"] == meta.user
    pd.testing.assert_frame_equal(pd.read_csv(output / prepared.file), frame)
    assert not (output / "lib").exists()
    parquet = tmp_path / "upload.parquet"
    frame.to_parquet(parquet, index=False)
    uploaded = local.pin_upload(parquet, "upload", data_dict=True)
    assert "source" not in yaml.safe_load(uploaded.user["data_dict"])["tables"][0]
    # A real CLI failure must abort rendering rather than use the default preview.
    bad = DICTIONARY.replace("integer", "not-a-data-dict-type")
    failed = tmp_path / "failed"
    failed.mkdir()
    with pytest.raises(PinsError, match="data-dict failed"):
        dd.render(bad, failed)
    assert not (failed / "index.html").exists()


def test_unsupported_generation_and_collision(tmp_path):
    with pytest.raises(PinsError, match="DataFrame"):
        dd.prepare_metadata(None, True, {"id": [1]})
    with pytest.raises(PinsError, match="Parquet"):
        dd.prepare_metadata(None, True, "data.csv", upload=True)
    index = tmp_path / "index.html"
    index.write_text("publisher's original")
    with pytest.raises(PinsError, match="overwrite index.html"):
        dd.render(DICTIONARY, tmp_path)
    assert index.read_text() == "publisher's original"


def test_missing_cli(monkeypatch):
    def missing(_):
        raise dd.PackageNotFoundError

    monkeypatch.setattr(dd, "distribution", missing)
    monkeypatch.setattr(dd.shutil, "which", lambda _: None)
    with pytest.raises(PinsError, match="Install pins"):
        dd._cli()


def test_cli_uses_executable_distribution_binary(tmp_path, monkeypatch):
    binary = tmp_path / "data-dict"
    binary.write_text("binary", encoding="utf-8")
    binary.chmod(0o755)

    class Package:
        files = [Path("data-dict")]

        def locate_file(self, _):
            return binary

    monkeypatch.setattr(dd, "distribution", lambda _: Package())
    monkeypatch.setattr(
        dd.shutil, "which", lambda _: pytest.fail("Unexpected PATH lookup")
    )

    assert dd._cli() == str(binary)


@pytest.mark.skipif(os.name == "nt", reason="POSIX executable bits are required")
def test_cli_repairs_non_executable_distribution_binary(tmp_path, monkeypatch):
    binary = tmp_path / "data-dict"
    binary.write_text("binary", encoding="utf-8")
    binary.chmod(0o644)

    class Package:
        files = [Path("data-dict")]

        def locate_file(self, _):
            return binary

    monkeypatch.setattr(dd, "distribution", lambda _: Package())
    monkeypatch.setattr(dd.shutil, "which", lambda _: None)

    assert dd._cli() == str(binary)
    assert binary.stat().st_mode & 0o100


@pytest.mark.skipif(os.name == "nt", reason="POSIX executable bits are required")
def test_cli_falls_back_to_path_when_bundled_binary_is_not_executable(
    tmp_path, monkeypatch
):
    binary = tmp_path / "data-dict"
    binary.write_text("binary", encoding="utf-8")
    binary.chmod(0o644)

    class Package:
        files = [Path("data-dict")]

        def locate_file(self, _):
            return binary

    monkeypatch.setattr(dd, "distribution", lambda _: Package())
    monkeypatch.setattr(dd.shutil, "which", lambda _: "/usr/local/bin/data-dict")

    assert dd._cli() == "/usr/local/bin/data-dict"
    assert binary.stat().st_mode & 0o100 == 0


@pytest.mark.skipif(os.name == "nt", reason="POSIX executable bits are required")
def test_cli_reports_failed_binary_repair(tmp_path, monkeypatch):
    binary = tmp_path / "data-dict"
    binary.write_text("binary", encoding="utf-8")
    binary.chmod(0o644)

    class Package:
        files = [Path("data-dict")]

        def locate_file(self, _):
            return binary

    monkeypatch.setattr(dd, "distribution", lambda _: Package())
    monkeypatch.setattr(dd.shutil, "which", lambda _: None)
    monkeypatch.setattr(
        Path,
        "chmod",
        lambda *args, **kwargs: (_ for _ in ()).throw(PermissionError("denied")),
    )

    with pytest.raises(PinsError, match="data-dict.*execute permission"):
        dd._cli()


def test_connect_parquet_profiles(tmp_path):
    _require_cli()
    pytest.importorskip("pyarrow")
    frame = pd.DataFrame(
        {"id": [1, 2, 3], "value": [10.0, None, 30.0], "group": ["a", "b", "a"]}
    )
    local = board_folder(tmp_path / "board")
    metadata = local.pin_write(frame, "data", type="parquet", data_dict=True).user
    connect = board_connect(
        server_url="https://example.com", api_key="offline", cache=None
    )
    output = tmp_path / "bundle"
    output.mkdir()
    meta = connect.prepare_pin_version(
        output, frame, "owner/data", type="parquet", metadata=metadata
    )
    overview = (output / "index.html").read_text()
    assert 'src="dictionary.html"' in overview
    assert "Download metadata" in overview and "Use this data" in overview
    html = (output / "dictionary.html").read_text()
    assert '<script src="theme.js"></script>' in html
    assert "#theme-toggle{display:none}" in html
    embedded = json.loads(
        re.search(r'<script[^>]*id="dict"[^>]*>(.*?)</script>', html, re.S)[1]
    )
    table = embedded["tables"][0]
    profiles = {column["name"]: column["profile"] for column in table["columns"]}
    assert table["rows"] == 3
    assert table["source"]["parquet"] == meta.file
    assert profiles["id"]["distinct"]["count"] == 3
    assert profiles["value"]["missing"] == 1
    assert profiles["value"]["range"] == {"min": 10.0, "max": 30.0}
    assert sum(bucket["count"] for bucket in profiles["value"]["histogram"]["bins"]) == 2
    assert profiles["group"]["common_values"]["values"] == [
        {"value": "a", "count": 2},
        {"value": "b", "count": 1},
    ]
    assert yaml.safe_load((output / "data.txt").read_text())["user"] == metadata
    assert "source" not in yaml.safe_load(metadata["data_dict"])["tables"][0]
    assert not list(output.glob("*.yaml"))
    assert (output / "pin.css").exists() and (output / "pin.js").exists()
    assert str(output) not in html
    pd.testing.assert_frame_equal(pd.read_parquet(output / meta.file), frame)


def test_cli_falls_back_to_path(monkeypatch):
    def missing(_):
        raise dd.PackageNotFoundError

    monkeypatch.setattr(dd, "distribution", missing)
    monkeypatch.setattr(dd.shutil, "which", lambda _: "/usr/local/bin/data-dict")
    assert dd._cli() == "/usr/local/bin/data-dict"


def test_required_cli_fails_when_missing(monkeypatch):
    monkeypatch.setenv("PINS_REQUIRE_DATA_DICT_CLI", "1")

    def missing():
        raise PinsError("data-dict is missing")

    monkeypatch.setattr(dd, "_cli", missing)
    with pytest.raises(PinsError, match="data-dict is missing"):
        _require_cli()


@pytest.mark.parametrize("file_bytes", [None, b"\xff"])
def test_data_dict_file_errors_are_pins_errors(tmp_path, file_bytes):
    path = tmp_path / "dictionary.yaml"
    if file_bytes is not None:
        path.write_bytes(file_bytes)

    with pytest.raises(PinsError) as error:
        dd.prepare_metadata(None, path, None)

    assert str(path) in str(error.value)
    assert "metadata['data_dict']" in str(error.value)


def test_draft_uses_absolute_uploaded_paths(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    arguments = []

    def draft(args):
        arguments.extend(args)
        Path(args[-1]).write_text(DICTIONARY, encoding="utf-8")

    monkeypatch.setattr(dd, "_run", draft)
    metadata = dd.prepare_metadata(None, True, "-data.parquet", upload=True)

    assert arguments[0] == "draft"
    assert arguments[1] == (tmp_path / "-data.parquet").resolve()
    assert "source" not in yaml.safe_load(metadata["data_dict"])["tables"][0]


def test_draft_accepts_multiple_uploaded_parquet_files(tmp_path, monkeypatch):
    paths = [tmp_path / "one.parquet", tmp_path / "two.parquet"]
    for path in paths:
        path.touch()
    arguments = []

    def draft(args):
        arguments.extend(args)
        Path(args[-1]).write_text(DICTIONARY, encoding="utf-8")

    monkeypatch.setattr(dd, "_run", draft)
    metadata = dd.prepare_metadata(None, True, paths, upload=True)

    assert arguments[0] == "draft"
    assert arguments[1:3] == paths
    assert "source" not in yaml.safe_load(metadata["data_dict"])["tables"][0]


def test_run_rejects_unsupported_cli_version_before_command(monkeypatch):
    calls = []
    monkeypatch.setattr(dd, "_cli", lambda: "data-dict")

    def run(args, **kwargs):
        calls.append(args)
        return dd.subprocess.CompletedProcess(args, 0, "data-dict 0.0.4\n", "")

    monkeypatch.setattr(dd.subprocess, "run", run)

    with pytest.raises(PinsError, match="requires data-dict 0.0.3"):
        dd._run(["render"])

    assert calls == [["data-dict", "--version"]]


@pytest.mark.parametrize(
    "error",
    [OSError("not executable"), dd.subprocess.TimeoutExpired("data-dict", 300)],
)
def test_run_os_and_timeout_errors_are_pins_errors(monkeypatch, error):
    monkeypatch.setattr(dd, "_cli", lambda: "data-dict")

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(dd.subprocess, "run", fail)
    with pytest.raises(PinsError, match="Could not run data-dict"):
        dd._run(["render"])


def test_render_write_failure_cleans_temporary_yaml(tmp_path, monkeypatch):
    temporary_files = []

    def fail_write(path, *args, **kwargs):
        temporary_files.append(path)
        raise OSError("disk full")

    monkeypatch.setattr(Path, "write_text", fail_write)
    with pytest.raises(OSError, match="disk full"):
        dd.render(DICTIONARY, tmp_path)

    assert len(temporary_files) == 1
    assert not temporary_files[0].exists()
    assert not (tmp_path / "index.html").exists()


def test_render_failure_cleans_temporary_yaml_and_leaves_index_absent(
    tmp_path, monkeypatch
):
    def fail(args):
        Path(args[-1]).write_text("partial render", encoding="utf-8")
        raise PinsError("renderer failed")

    monkeypatch.setattr(dd, "_run", fail)
    with pytest.raises(PinsError, match="renderer failed"):
        dd.render(DICTIONARY, tmp_path)

    assert not list(tmp_path.glob("*.yaml"))
    assert not (tmp_path / "index.html").exists()


@pytest.mark.parametrize(
    ("files", "dictionary", "expected_source"),
    [
        (["one.parquet", "two.parquet"], DICTIONARY, None),
        (["one.parquet"], MULTI_TABLE_DICTIONARY, None),
        (["DATA.PARQUET"], DICTIONARY, {"parquet": "DATA.PARQUET"}),
        (["one.csv"], DICTIONARY, None),
    ],
)
def test_render_profiles_only_one_parquet_table(
    tmp_path, monkeypatch, files, dictionary, expected_source
):
    rendered = {}

    def render_cli(args):
        rendered["document"] = yaml.safe_load(Path(args[1]).read_text(encoding="utf-8"))
        Path(args[3]).write_text(
            "<html><head></head><body>rendered</body></html>", encoding="utf-8"
        )

    monkeypatch.setattr(dd, "_run", render_cli)
    dd.render(dictionary, tmp_path, files)

    tables = rendered["document"]["tables"]
    if expected_source is None:
        assert all("source" not in table for table in tables)
    else:
        assert tables[0]["source"] == expected_source
    assert (tmp_path / "index.html").is_file()


def test_render_preserves_existing_dangling_symlink(tmp_path):
    index = tmp_path / "dictionary.html"
    try:
        index.symlink_to(tmp_path / "missing.html")
    except OSError:
        pytest.skip("Symlinks are unavailable on this filesystem")
    with pytest.raises(PinsError, match="overwrite"):
        dd.render(
            "tables: [{name: data, columns: []}]", tmp_path, filename="dictionary.html"
        )
    assert index.is_symlink()
