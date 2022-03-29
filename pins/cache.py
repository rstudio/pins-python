import humanize
import os
import time
import shutil

from fsspec.implementations.cached import SimpleCacheFileSystem, hash_name
from fsspec import register_implementation
from pathlib import Path

from .config import get_cache_dir


def touch_access_time(path, access_time: "float | None" = None):
    """Update access time of file.

    Returns the new access time.
    """

    if access_time is None:
        access_time = time.time()

    p = Path(path)

    if not p.exists():
        p.touch()

    stat = p.stat()
    os.utime(path, (access_time, stat.st_mtime))

    return access_time


class PinsCache(SimpleCacheFileSystem):
    protocol = "pinscache"

    def _make_local_details(self, path):
        # modifies method to create any parent directories needed by the cached file
        # note that this is called in ._open(), at the point it's known the file
        # will be cached
        fn = super()._make_local_details(path)
        Path(fn).parent.mkdir(parents=True, exist_ok=True)

        return fn

    def hash_name(self, path, same_name):
        # the main change in this function is that, for same_name, it returns
        # the full path
        if same_name:
            hash = path
        else:
            hash = hash_name(path, same_name)

        return hash

    def touch_access_time(path):
        return touch_access_time(path)


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

        print("Skipping cache deletion")


def delete_version(path: "str | Path"):
    path = Path(path)
    shutil.rmtree(str(path.absolute()))


def disk_usage(path):
    return sum(p.stat().st_size for p in path.glob("**/*") if p.is_file() or p.is_dir())


def prompt_cache_prune(to_prune, size) -> bool:
    print(to_prune)
    human_size = humanize.naturalsize(size, binary=True)
    resp = input(f"Delete {len(to_prune)} pin versions, freeing {human_size}?")
    return resp == "yes"


def cache_prune(days=30, cache_root=None, prompt=True):
    if cache_root is None:
        cache_root = get_cache_dir()

    final_delete = []
    for p_board in Path(cache_root).glob("*"):
        pruner = CachePruner(p_board)
        final_delete.extend(pruner.old_versions(days))

    size = sum(map(disk_usage, final_delete))

    if prompt:
        confirmed = prompt_cache_prune(final_delete, size)
    else:
        confirmed = True
    if confirmed:
        for p in final_delete:
            delete_version(p)


# def prune_files(days = 30, path = None):
#     if path is None:
#         for p_cache in Path(get_cache_dir()).glob("*"):
#             return prune_files(days=days, path=str(p_cache.absolute()))
#
#     expiry_time_sec = days * 60 * 60 * 24
#     fs_cache = PinsCache(
#         target_protocol=None,
#         cache_storage=path,
#         check_files=True,
#         expiry_time=expiry_time_sec
#     )
#
#     # note that fsspec considers only the last entry in cached_files deletable
#     for hash_path, entry in fs_cache.cached_files[-1].items():
#         if time.time() - detail["time"] > self.expiry:
#             fs_cache.pop_from_cache(entry["original"])

# TODO: swap to use entrypoint
register_implementation("pinscache", PinsCache)
