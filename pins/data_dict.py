"""Optional data dictionary generation and Connect rendering."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

import pandas as pd
import yaml

from pins.errors import PinsError


def _cli():
    try:
        package = distribution("data-dict-yaml")
        for file in package.files or []:
            if file.name in ("data-dict", "data-dict.exe"):
                binary = package.locate_file(file)
                if binary.is_file():
                    return str(binary)
    except PackageNotFoundError:
        pass
    binary = shutil.which("data-dict")
    if binary:
        return binary
    raise PinsError(
        "Data dictionary support requires the data-dict CLI. "
        "Install pins[data-dict] or put data-dict on PATH."
    )


def _run(args):
    try:
        subprocess.run(
            [_cli(), *map(str, args)],
            check=True,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.CalledProcessError as error:
        raise PinsError(f"data-dict failed: {error.stderr.strip()}") from error
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PinsError(f"Could not run data-dict: {error}") from error


def _parse(text):
    if not isinstance(text, str):
        raise PinsError("metadata['data_dict'] must contain YAML text.")
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as error:
        raise PinsError(f"Invalid data dictionary YAML: {error}") from error
    if not isinstance(document, dict) or not isinstance(document.get("tables"), list):
        raise PinsError("A data dictionary must contain a tables list.")
    if not document["tables"] or any(
        not isinstance(table, dict) for table in document["tables"]
    ):
        raise PinsError("A data dictionary must contain at least one table mapping.")
    return document


def _parse_portable(text):
    document = _parse(text)
    if any("source" in table for table in document["tables"]):
        raise PinsError("Pin data dictionaries must omit table source fields.")
    return document


def _draft(paths, output):
    paths = [Path(path).resolve() for path in paths]
    _run(["draft", *paths, "-o", output])
    return _parse(output.read_text(encoding="utf-8"))


def prepare_metadata(metadata, data_dict, x, *, upload=False):
    """Attach portable YAML without changing the caller's metadata."""
    if data_dict is None and (metadata is None or "data_dict" not in metadata):
        return metadata
    result = dict(metadata or {})
    if data_dict is not None and "data_dict" in result:
        raise PinsError("Supply data_dict or metadata['data_dict'], not both.")
    if data_dict is True:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "data-dict.yaml"
            if upload:
                paths = [x] if isinstance(x, (str, Path)) else list(x)
                if not paths or any(Path(p).suffix.lower() != ".parquet" for p in paths):
                    raise PinsError(
                        "data_dict=True for pin_upload requires Parquet files."
                    )
            else:
                if not isinstance(x, pd.DataFrame):
                    raise PinsError("data_dict=True for pin_write requires a DataFrame.")
                paths = [Path(temporary) / "data.parquet"]
                try:
                    x.to_parquet(paths[0], engine="pyarrow", index=False)
                except ImportError as error:
                    raise PinsError(
                        "Drafting a dictionary requires pins[data-dict]."
                    ) from error
            document = _draft(paths, output)
            for table in document["tables"]:
                table.pop("source", None)
            result["data_dict"] = yaml.safe_dump(document, sort_keys=False)
    elif isinstance(data_dict, (str, Path)):
        path = Path(data_dict)
        try:
            result["data_dict"] = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            raise PinsError(
                f"Could not read data dictionary file {path}: {error}. "
                "Supply raw YAML in metadata['data_dict'] instead."
            ) from error
    elif data_dict is not None:
        raise PinsError("data_dict must be None, True, or a YAML file path.")
    _parse_portable(result["data_dict"])
    return result


def render(text, directory, files=(), *, filename="index.html"):
    """Render a self-contained index without bundling temporary YAML."""
    document = _parse_portable(text)
    index = Path(directory) / filename
    if index.exists() or index.is_symlink():
        raise PinsError(f"Generating a data dictionary would overwrite {filename}.")
    files = [files] if isinstance(files, str) else list(files)
    # shortcut: Profiles cover one Parquet file and one table; add explicit mapping for multiple tables.
    if (
        len(files) == 1
        and Path(files[0]).suffix.lower() == ".parquet"
        and len(document["tables"]) == 1
    ):
        document["tables"][0]["source"] = {"parquet": files[0]}
        text = yaml.safe_dump(document, sort_keys=False)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".yaml", dir=directory, delete=False
    ) as temporary:
        dictionary = Path(temporary.name)
    try:
        dictionary.write_text(text, encoding="utf-8")
        _run(["render", dictionary, "-o", index])
        if not index.is_file():
            raise PinsError(f"data-dict did not produce {filename}.")
        if filename == "dictionary.html":
            html = index.read_text(encoding="utf-8")
            integration = '<script src="theme.js"></script><style>#theme-toggle{display:none}</style>'
            index.write_text(
                html.replace("</head>", integration + "</head>", 1), encoding="utf-8"
            )
    except Exception:
        index.unlink(missing_ok=True)
        raise
    finally:
        dictionary.unlink(missing_ok=True)
