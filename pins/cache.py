from fsspec.implementations.cached import WholeFileCacheFileSystem, hash_name
from fsspec import register_implementation
from pathlib import Path


class PinsCache(WholeFileCacheFileSystem):
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


# TODO: swap to use entrypoint
register_implementation("pinscache", PinsCache)
