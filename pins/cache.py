import humanize
import logging
import os
import time
import shutil
import urllib.parse

from fsspec.implementations.cached import SimpleCacheFileSystem, hash_name
from fsspec import register_implementation
from pathlib import Path

from .config import get_cache_dir
from .utils import inform

_log = logging.getLogger(__name__)


# used if needed to preserve board path structure in the cache
PLACEHOLDER_VERSION = "v"
PLACEHOLDER_FILE = "file"


def touch_access_time(path, access_time: "float | None" = None, strict=True):
    """Update access time of file.

    Returns the new access time.
    """

    if access_time is None:
        access_time = time.time()

    p = Path(path)

    if not p.exists() and not strict:
        p.touch()

    stat = p.stat()
    os.utime(path, (access_time, stat.st_mtime))

    return access_time


def protocol_to_string(protocol):
    if isinstance(protocol, str):
        return protocol

    return protocol[0]


class PinsCache(SimpleCacheFileSystem):
    protocol = "pinscache"

    def __init__(self, *args, hash_prefix=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.hash_prefix = hash_prefix

    def _open(self, path, *args, **kwargs):
        # For some reason, the open method of SimpleCacheFileSystem doesn't
        # call _make_local_details, so we need to patch in here.
        # Note that methods like .cat() do call it. Other Caches don't have this issue.
        path = self._strip_protocol(path)
        self._make_local_details(path)

        return super()._open(path, *args, **kwargs)

    def _make_local_details(self, path):
        # modifies method to create any parent directories needed by the cached file
        # note that this is called in ._open(), at the point it's known the file
        # will be cached
        fn = super()._make_local_details(path)
        _log.info(f"cache file: {fn}")
        Path(fn).parent.mkdir(parents=True, exist_ok=True)

        return fn

    def hash_name(self, path, same_name):
        # the main change in this function is that, for same_name, it returns
        # the full path
        if same_name:
            if self.hash_prefix is not None:
                # optionally make the name relative to a parent path
                # using the hash of parent path as a prefix, to flatten a bit
                suffix = Path(path).relative_to(Path(self.hash_prefix))
                # TODO(compat): R pins uses xxh128 hash here, but fsspec uses sha256
                prefix = hash_name(self.hash_prefix, False)

                # TODO: hacky to automatically tack on protocol here
                # but this is what R pins boards do. Could make a bool arg?
                proto_name = protocol_to_string(self.fs.protocol)
                full_prefix = "_".join([proto_name, prefix])
                return str(full_prefix / suffix)

            return path
        else:
            raise NotImplementedError()


class PinsRscCache(PinsCache):
    """Modifies the PinsCache to allow hash_prefix to be an RSC server url.

    Note that this class also modifies the first / in a path to be a -, so that
    pin contents will not be put into subdirectories, for e.g. michael/mtcars/data.txt.
    """

    protocol = "pinsrsccache"

    def hash_name(self, path, same_name):
        # the main change in this function is that, for same_name, it returns
        # the full path
        if same_name:
            if self.hash_prefix is None:
                raise NotImplementedError()

            # change pin path of form <user>/<content> to <user>+<content>
            suffix = path.replace("/", "+", 1)
            prefix = hash_name(self.hash_prefix, False)

            # TODO: hacky to automatically tack on protocol here
            # but this is what R pins boards do. Could make a bool arg?
            proto_name = protocol_to_string(self.fs.protocol)
            full_prefix = "_".join([proto_name, prefix])
            return str(full_prefix / Path(suffix))

        else:
            raise NotImplementedError()


class PinsUrlCache(PinsCache):
    protocol = "pinsurlcache"

    def hash_name(self, path, same_name):
        # strip final arg from path
        # note that R pins uses fs::path_file, and I'm not sure exactly how it
        # behaves for the many forms url paths can take.
        # e.g. path_file(.../extdata/) -> extdata
        # e.g. path_file(.../extdata?123) -> extdata?123
        path_parts = urllib.parse.urlparse(path)[2]

        # strip off final whitespace and / (if it exists)
        # TODO(compat): python pins currently not keeping query part of url
        final_part = path_parts.rstrip().rstrip("/").rsplit("/", 1)[-1]

        # TODO: what happens in R pins if no final part?
        if final_part == "":
            final_part = PLACEHOLDER_FILE

        # hash url
        prefix = hash_name(path, False)

        # note that we include an extra version folder, so it conforms with
        # pin board path form: <board_path>/<pin_name>/<version_name>/<file>
        proto_name = protocol_to_string(self.fs.protocol)
        full_prefix = "_".join([proto_name, prefix])
        return str(Path(full_prefix) / PLACEHOLDER_VERSION / final_part)


class CachePruner:
    """Prunes the cache directory, across multiple boards.

    Note
    ----

    `pins` assumes that all boards cache using these rules:

    * path structure: `<cache_root>/<board_hash>/<pin>/<version>`.
    * each version has a data.txt file in it.
    """

    meta_path = "data.txt"

    def __init__(self, cache_dir: "str | Path"):
        self.cache_dir = Path(cache_dir)

    def versions(self) -> "iter[Path]":
        for p_version in self.cache_dir.glob("*/*"):
            if p_version.is_dir() and (p_version / self.meta_path).exists():
                yield p_version

    def should_prune_version(self, days, path: "str | Path"):
        path = Path(path)

        expiry_time_sec = days * 60 * 60 * 24
        prune_before = time.time() - expiry_time_sec

        p_meta = path / self.meta_path

        if not p_meta.exists():
            raise FileNotFoundError(f"No metadata file: {p_meta.absolute()}")

        access_time = p_meta.stat().st_atime
        return access_time < prune_before

    def old_versions(self, days):
        return [p for p in self.versions() if self.should_prune_version(days, p)]

    def prune(self, days=30):
        to_prune = self.old_versions(days)
        size = sum(map(disk_usage, to_prune))

        # TODO: clean this up, general approach to prompting
        confirmed = prompt_cache_prune(to_prune, size)
        if confirmed:
            for path in to_prune:
                delete_version(to_prune)

        _log.info("Skipping cache deletion")


def delete_version(path: "str | Path"):
    path = Path(path)
    shutil.rmtree(str(path.absolute()))


def disk_usage(path):
    return sum(p.stat().st_size for p in path.glob("**/*") if p.is_file() or p.is_dir())


def prompt_cache_prune(to_prune, size) -> bool:
    _log.info(f"Pruning items: {to_prune}")
    human_size = humanize.naturalsize(size, binary=True)
    resp = input(
        f"Delete {len(to_prune)} pin versions, freeing {human_size}?"
        "\n1: Yes"
        "\n2: No"
        "\n\nSelection: "
    )
    return resp == "1"


def cache_info():
    cache_root = get_cache_dir()

    cache_boards = list(Path(cache_root).glob("*"))

    print(f"Cache info: {cache_root}")
    for p_board in cache_boards:
        du = disk_usage(p_board)
        human_size = humanize.naturalsize(du, binary=True)
        rel_path = p_board.relative_to(cache_root)
        print(f"* {rel_path}: {human_size}")


def cache_prune(days=30, cache_root=None, prompt=True):
    if cache_root is None:
        cache_root = get_cache_dir()

    final_delete = []
    for p_board in Path(cache_root).glob("*"):
        pruner = CachePruner(p_board)
        final_delete.extend(pruner.old_versions(days))

    size = sum(map(disk_usage, final_delete))

    if not final_delete:
        inform(_log, "No stale pins found")

    if prompt:
        confirmed = prompt_cache_prune(final_delete, size)
    else:
        confirmed = True

    if confirmed:
        inform(_log, "Deleting pins from cache.")
        for p in final_delete:
            delete_version(p)
    else:
        inform(_log, "Skipping deletion of pins from cache.")


# TODO: swap to use entrypoint
register_implementation("pinscache", PinsCache)
