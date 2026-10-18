"""Where step results too big for a journal row go (default: > 256 KB).

The journal keeps `{"result_ref": key}`; the server puts the value back when it hands the
journal to a worker. Filesystem-backed here; an S3 store implements the same two methods.
"""

import json
import os
import tempfile
from typing import Any, Protocol

MAX_INLINE = int(os.environ.get("RELAY_MAX_INLINE_BYTES", 256 * 1024))


class BlobStore(Protocol):
    max_inline: int

    def put(self, key: str, value: Any) -> None: ...
    def get(self, key: str) -> Any: ...


class FileBlobStore:
    def __init__(self, root: str, max_inline: int = MAX_INLINE) -> None:
        self.root = root
        self.max_inline = max_inline
        os.makedirs(root, exist_ok=True)

    def _path(self, key: str) -> str:
        return os.path.join(self.root, key.replace("/", "__") + ".json")

    def put(self, key: str, value: Any) -> None:
        # write-then-rename: a crash never leaves a half-written blob behind a journal ref
        fd, tmp = tempfile.mkstemp(dir=self.root)
        with os.fdopen(fd, "w") as f:
            json.dump(value, f)
        os.replace(tmp, self._path(key))

    def get(self, key: str) -> Any:
        with open(self._path(key)) as f:
            return json.load(f)


def blob_store_from_env() -> BlobStore:
    return FileBlobStore(os.environ.get("RELAY_BLOB_DIR", os.path.abspath(".relay-blobs")))
