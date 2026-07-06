from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import shutil
import tempfile
import time

from collections.abc import Callable
from pathlib import Path

from .exceptions import HctefNetworkError

logger = logging.getLogger(__name__)

DEFAULT_BLOCK_BYTES = 2**20  # 1 MiB
_TOUCH_INTERVAL_SECONDS = 60.0
_TRUTHY = frozenset({'1', 'true', 'yes', 'on'})


def _env_truthy(value: str | None) -> bool:
    return value is not None and value.strip().lower() in _TRUTHY


def _resolve_block_size(block_size: int | None) -> int:
    if block_size is not None:
        return block_size
    env = os.environ.get('HCTEF_CACHE_BLOCK_BYTES')
    if env:
        return int(env)
    return DEFAULT_BLOCK_BYTES


def _resolve_max_bytes(max_bytes: int | None) -> int | None:
    if max_bytes is not None:
        return max_bytes
    env = os.environ.get('HCTEF_CACHE_MAX_BYTES')
    if env:
        return int(env)
    return None


def _resolve_immutable(immutable: bool | None) -> bool:
    if immutable is not None:
        return immutable
    return _env_truthy(os.environ.get('HCTEF_CACHE_IMMUTABLE'))


class _BlockStore:
    """
    Disk-backed, fixed-size block cache for a single URL.

    The OS page cache is the sole in-memory tier: no fetched data bytes are
    retained in Python beyond transient request/assembly buffers. Blocks are
    stored one file per block under a per-URL directory so that a crash can
    corrupt at most a single (atomically written) block.

    This base class holds all pure block math, disk I/O, presence scanning,
    coalescing, assembly and eviction. Fetching is left to the sync and async
    subclasses so a single implementation serves both.
    """

    def __init__(
        self,
        url: str,
        file_size: int,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
        cache_dir: str | os.PathLike[str] | None = None,
        block_size: int | None = None,
        max_bytes: int | None = None,
        immutable: bool | None = None,
    ) -> None:
        if file_size < 0:
            raise ValueError('File size cannot be less than zero')

        self.url = url
        self.file_size = file_size
        self.block_size = _resolve_block_size(block_size)
        if self.block_size <= 0:
            raise ValueError('Block size must be a positive integer')
        self.max_bytes = _resolve_max_bytes(max_bytes)
        self.immutable = _resolve_immutable(immutable)

        # Configuration precedence: explicit arg, then env var, then a
        # TemporaryDirectory cleaned up on close(). The tmp dir replaces the
        # old in-memory cache mode entirely.
        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None
        configured = (
            cache_dir
            if cache_dir is not None
            else os.environ.get(
                'HCTEF_CACHE_DIR',
            )
        )
        if configured is None:
            self._tmpdir = tempfile.TemporaryDirectory(prefix='hctef-')
            self.cache_dir = Path(self._tmpdir.name)
        else:
            self.cache_dir = Path(configured)

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(url.encode('utf-8')).hexdigest()[:16]
        self.url_dir = self.cache_dir / digest

        # LRU bookkeeping: last time we bumped a block's mtime on read.
        self._touched: dict[int, float] = {}

        self._etag = etag
        self._last_modified = last_modified
        self._open_meta()

    # -- meta -------------------------------------------------------------

    @property
    def _meta_path(self) -> Path:
        return self.url_dir / 'meta.json'

    def _current_meta(self) -> dict[str, object]:
        return {
            'url': self.url,
            'size': self.file_size,
            'etag': self._etag,
            'last_modified': self._last_modified,
            'block_size': self.block_size,
        }

    def _meta_matches(self, meta: dict[str, object]) -> bool:
        # block_size is a layout invariant regardless of immutability.
        if meta.get('block_size') != self.block_size:
            return False
        if self.immutable:
            return True
        return (
            meta.get('size') == self.file_size
            and meta.get('etag') == self._etag
            and meta.get('last_modified') == self._last_modified
        )

    def _open_meta(self) -> None:
        if self._meta_path.is_file():
            try:
                meta = json.loads(self._meta_path.read_text())
            except (OSError, ValueError):
                meta = None
            if meta is not None and self._meta_matches(meta):
                return
            # Stale or unreadable: wipe this URL's dir and start fresh.
            logger.debug('Cache meta mismatch for %s; wiping dir', self.url)
            shutil.rmtree(self.url_dir, ignore_errors=True)

        self.url_dir.mkdir(parents=True, exist_ok=True)
        self._atomic_write(self._meta_path, json.dumps(self._current_meta()).encode())

    # -- block paths / math ----------------------------------------------

    def _block_path(self, index: int) -> Path:
        return self.url_dir / f'{index:010d}.blk'

    def _block_indices(self, start: int, end: int) -> range:
        if end <= start:
            return range(0)
        return range(start // self.block_size, (end - 1) // self.block_size + 1)

    def _run_byte_range(self, first: int, last: int) -> tuple[int, int]:
        start = first * self.block_size
        end = min((last + 1) * self.block_size, self.file_size)
        return start, end

    @staticmethod
    def _coalesce(missing: list[int]) -> list[tuple[int, int]]:
        """Group a sorted list of block indices into contiguous runs."""
        runs: list[tuple[int, int]] = []
        for index in missing:
            if runs and index == runs[-1][1] + 1:
                runs[-1] = (runs[-1][0], index)
            else:
                runs.append((index, index))
        return runs

    def _missing_blocks(self, indices: range) -> list[int]:
        return [i for i in indices if not self._block_path(i).exists()]

    def _check_fetched(self, start: int, end: int, data: bytes) -> bytes:
        """
        Refuse wrong-sized fetch results before they reach the disk cache.

        A wrong-sized body means a bad response -- a server that ignored the
        Range header and sent the whole file, or a truncated transfer.
        Guarding here covers every transport (including injected ones) at the
        single point where bytes are about to be persisted.
        """
        if len(data) != end - start:
            raise HctefNetworkError(
                f'Fetched {len(data)} bytes for range {start}-{end} of '
                f'{self.url} (expected {end - start}); refusing to cache',
            )
        return data

    # -- disk I/O ---------------------------------------------------------

    def _atomic_write(self, dest: Path, data: bytes) -> None:
        """
        Write data to dest via a tmp file in the same dir + os.replace, so a
        crash or concurrent writer can never leave a torn file. Last writer
        wins harmlessly since every writer writes identical block bytes.
        """
        fd, tmp = tempfile.mkstemp(dir=dest.parent, suffix='.tmp')
        try:
            with os.fdopen(fd, 'wb') as fh:
                fh.write(data)
            os.replace(tmp, dest)  # noqa: PTH105  (atomic, spied on in tests)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def _write_run(self, first: int, data: bytes, protected: set[int]) -> None:
        """Split a fetched run into blocks, writing each atomically."""
        for offset in range(0, len(data), self.block_size):
            index = first + offset // self.block_size
            self._atomic_write(
                self._block_path(index),
                data[offset : offset + self.block_size],
            )
            self._evict(protected)

    def _assemble(self, start: int, end: int) -> bytes:
        """
        Assemble the result by seeking/reading directly from block files.

        These are plain synchronous file reads even under the async wrapper:
        a present block is an OS page-cache hit measured in microseconds,
        cheaper than a thread-pool hop, so offloading would only add latency.
        """
        out = bytearray()
        for index in self._block_indices(start, end):
            block_start = index * self.block_size
            read_from = max(start, block_start) - block_start
            read_to = min(end, block_start + self.block_size) - block_start
            with self._block_path(index).open('rb') as fh:
                fh.seek(read_from)
                out += fh.read(read_to - read_from)
        return bytes(out)

    def _touch(self, indices: range) -> None:
        """Bump block mtimes for LRU, at most once per block per interval."""
        now = time.time()
        for index in indices:
            last = self._touched.get(index, 0.0)
            if now - last < _TOUCH_INTERVAL_SECONDS:
                continue
            self._touched[index] = now
            with contextlib.suppress(OSError):
                os.utime(self._block_path(index))

    # -- eviction ---------------------------------------------------------

    def _evict(self, protected: set[int]) -> None:
        """
        Enforce max_bytes over the WHOLE cache_dir by deleting least-recently
        used .blk files (LRU by mtime). Blocks fetched for the in-flight read
        of the current URL are never evicted.
        """
        if self.max_bytes is None:
            return

        protected_paths = {self._block_path(i) for i in protected}
        blocks: list[tuple[float, int, Path]] = []
        total = 0
        for path in self.cache_dir.rglob('*.blk'):
            try:
                stat = path.stat()
            except OSError:
                continue
            total += stat.st_size
            blocks.append((stat.st_mtime, stat.st_size, path))

        if total <= self.max_bytes:
            return

        blocks.sort(key=lambda item: item[0])
        for _, size, path in blocks:
            if total <= self.max_bytes:
                break
            if path in protected_paths:
                continue
            try:
                path.unlink()
            except OSError:
                continue
            total -= size

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        if self._tmpdir is not None:
            self._tmpdir.cleanup()
            self._tmpdir = None

    def __enter__(self) -> _BlockStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class BlockCache(_BlockStore):
    """Synchronous disk-backed block cache used directly by HttpFile."""

    def __init__(
        self,
        url: str,
        file_size: int,
        fetch: Callable[[int, int], bytes],
        **kwargs: object,
    ) -> None:
        super().__init__(url, file_size, **kwargs)  # type: ignore[arg-type]
        self._fetch = fetch

    def read(self, start: int, end: int) -> bytes:
        if end > self.file_size:
            raise ValueError('Read request extends beyond the end of the file.')
        if end <= start:
            return b''

        indices = self._block_indices(start, end)
        protected: set[int] = set()
        for first, last in self._coalesce(self._missing_blocks(indices)):
            fetch_start, fetch_end = self._run_byte_range(first, last)
            protected.update(range(first, last + 1))
            data = self._check_fetched(
                fetch_start,
                fetch_end,
                self._fetch(fetch_start, fetch_end),
            )
            self._write_run(first, data, protected)

        self._touch(indices)
        return self._assemble(start, end)
