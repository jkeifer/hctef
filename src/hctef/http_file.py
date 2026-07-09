from __future__ import annotations

import urllib.error
import urllib.request
import warnings

from collections.abc import Iterable
from typing import Literal, Self

from .block_cache import BlockCache
from .exceptions import (
    HctefNetworkError,
    HctefUrlError,
    RangeRequestsUnsupportedError,
)

# urllib has no default timeout, so a dead connection would block forever.
DEFAULT_TIMEOUT_SECONDS = 30.0


def _check_url(url: str) -> None:
    """
    Validate that URL is a valid HTTP/HTTPS URL.

    Args:
        url: URL to validate

    Raises:
        HctefUrlError: If URL doesn't start with http: or https:
    """
    if not url.startswith(('http:', 'https:')):
        raise HctefUrlError("URL must start with 'http:' or 'https:'")


class _OpenedHttpFile:
    """
    Internal class of HttpFile for managing state while open
    """

    def __init__(
        self,
        http_file: HttpFile,
    ) -> None:
        """
        Open the HTTP URL for the HttpFile.

        Args:
            http_file: An HttpFile instance.

        Raises:
            HctefNetworkError: If server doesn't support range requests
        """

        self.http_file = http_file
        self._position = 0
        self._etag: str | None = None
        self._last_modified: str | None = None
        self._size = self._get_file_size()
        self._cache = BlockCache(
            self.http_file.url,
            self._size,
            self._fetch_range,
            etag=self._etag,
            last_modified=self._last_modified,
            cache_dir=self.http_file._cache_dir,
            block_size=self.http_file._block_size,
            max_bytes=self.http_file._max_bytes,
            immutable=self.http_file._immutable,
        )

        prefetch_bytes = min(
            self.http_file._prefetch_bytes,
            self._size,
        )
        prefetch_direction = self.http_file._prefetch_direction
        if prefetch_bytes > 0 and prefetch_direction == 'START':
            self._cache.read(0, prefetch_bytes)
        elif prefetch_bytes > 0 and prefetch_direction == 'END':
            self._cache.read(self._size - prefetch_bytes, self._size)

    def _get_file_size(self) -> int:
        """
        Get total file size using HTTP range request.

        Returns:
            File size in bytes

        Raises:
            HctefNetworkError: If size cannot be determined
        """
        try:
            # One-byte range: the Content-Range total is the same, and the
            # server never starts streaming the whole body just for a probe.
            request = urllib.request.Request(  # noqa: S310
                self.http_file.url,
                headers={'Range': 'bytes=0-0'},
            )
            with urllib.request.urlopen(  # noqa: S310
                request,
                timeout=self.http_file._timeout,
            ) as response:
                self._etag = response.headers.get('ETag')
                self._last_modified = response.headers.get('Last-Modified')
                content_range = response.headers.get('Content-Range')
                if content_range:
                    return int(content_range.split('/')[-1])

                # If no Content-Range header, server doesn't support ranges
                raise RangeRequestsUnsupportedError(
                    f'Server does not support range requests for {self.http_file.url}',
                    reason='no-range-support',
                )
        except HctefNetworkError:
            raise
        except urllib.error.HTTPError as e:
            raise HctefNetworkError(
                f'HTTP {e.code} probing {self.http_file.url}',
            ) from e
        except Exception as e:
            raise HctefNetworkError(
                f'Cannot determine file size for {self.http_file.url}',
            ) from e

    def read(self, size: int | None = None, /) -> bytes:
        if size is None:
            size = self._size - self._position

        if size < 0:
            raise ValueError(f'Cannot read negative number of bytes, got: {size}')

        if size == 0:
            return b''

        start = self._position
        end = min(start + size, self._size)

        data = self._cache.read(start, end)

        self._position = end
        return data

    def _fetch_range(self, start: int, end: int) -> bytes:
        """
        Fetch byte range using HTTP request and add to cache.

        Args:
            start: Start byte position (inclusive)
            end: End byte position (exclusive)

        Raises:
            HctefNetworkError: If range request fails
        """
        if start >= end or start < 0 or end > self._size:
            raise HctefUrlError(
                f'Invalid byte range: {start}-{end} (file size: {self._size})',
            )

        try:
            request = urllib.request.Request(  # noqa: S310
                self.http_file.url,
                headers={'Range': f'bytes={start}-{end - 1}'},
            )
            with urllib.request.urlopen(  # noqa: S310
                request,
                timeout=self.http_file._timeout,
            ) as response:
                if response.status == 200:
                    # The server ignored the Range header; its full body
                    # must never be cached as if it were the slice.
                    raise RangeRequestsUnsupportedError(
                        f'Server ignored the Range header fetching bytes '
                        f'{start}-{end} from {self.http_file.url} (got 200)',
                        reason='no-range-support',
                    )
                if response.status != 206:
                    raise HctefNetworkError(
                        f'Expected 206 Partial Content fetching bytes '
                        f'{start}-{end} from {self.http_file.url}, '
                        f'got {response.status}',
                    )
                return response.read()
        except HctefNetworkError:
            raise
        except Exception as e:
            raise HctefNetworkError(
                f'Failed to fetch bytes {start}-{end} from {self.http_file.url}:',
            ) from e

    def seek(self, offset: int, whence: int = 0, /) -> int:
        if whence == 0:  # Absolute position
            new_pos = offset
        elif whence == 1:  # Relative to current position
            new_pos = self._position + offset
        elif whence == 2:  # Relative to end
            new_pos = self._size + offset
        else:
            raise ValueError(f'Invalid whence value: {whence}')

        if new_pos < 0:
            new_pos = 0
        elif new_pos > self._size:
            new_pos = self._size

        self._position = new_pos
        return self._position

    def tell(self) -> int:
        return self._position


class HttpFile:
    """
    File-like wrapper for HTTP URLs with range request support.
    """

    def __init__(
        self,
        url: str,
        prefetch_bytes: int = 2**20,
        prefetch_direction: Literal['START', 'END'] = 'END',
        cache_dir: str | None = None,
        block_size: int | None = None,
        max_bytes: int | None = None,
        immutable: bool | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        minimum_range_request_bytes: int | None = None,
    ) -> None:
        """
        Initialize HTTP file wrapper.

        Args:
            url: HTTP/HTTPS URL for a file

        Keyword Args:
            prefetch_bytes:
                How many bytes to request when initializing the class.
                Set to 0 or less to disable prefetch. Default 1 MiB.
            prefetch_direction:
                Whether to prefetch from file start or file end.
                Possible values `START` or `END`.
            cache_dir:
                Directory for the disk-backed block cache. Falls back to
                ``HCTEF_CACHE_DIR`` then a per-process temporary directory.
            block_size:
                Fixed block size in bytes. Falls back to
                ``HCTEF_CACHE_BLOCK_BYTES`` then 1 MiB.
            max_bytes:
                Optional cap on the whole cache dir, enforced by LRU eviction.
                Falls back to ``HCTEF_CACHE_MAX_BYTES``.
            immutable:
                Skip etag/last-modified validation. Falls back to
                ``HCTEF_CACHE_IMMUTABLE``.
            timeout:
                Socket timeout in seconds for each HTTP request.
            minimum_range_request_bytes:
                Deprecated and ignored; ``block_size`` subsumes request
                coalescing.

        Raises:
            HctefUrlError: If URL is invalid
            HctefNetworkError: If server doesn't support range requests
        """

        if minimum_range_request_bytes is not None:
            warnings.warn(
                'minimum_range_request_bytes is deprecated and ignored; '
                'use block_size instead',
                DeprecationWarning,
                stacklevel=2,
            )

        _check_url(url)
        self.url = url
        self._prefetch_bytes = prefetch_bytes
        self._prefetch_direction = prefetch_direction
        self._cache_dir = cache_dir
        self._block_size = block_size
        self._max_bytes = max_bytes
        self._immutable = immutable
        self._timeout = timeout
        self._opened: _OpenedHttpFile | None = None

    @property
    def _ohf(self) -> _OpenedHttpFile:
        if not self._opened:
            raise ValueError('I/O operation on closed file')
        return self._opened

    def open(self) -> Self:
        self._opened = _OpenedHttpFile(self)
        return self

    def close(self) -> None:
        """
        Close the file, releasing any temporary cache directory.
        """
        if self._opened is not None:
            self._opened._cache.close()
        self._opened = None

    def __enter__(self) -> Self:
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __repr__(self) -> str:
        if self._opened:
            return (
                f'HttpFile(url={self.url!r}, opened=True, '
                f'size={self._ohf._size}, pos={self._ohf._position})'
            )
        return f'HttpFile(url={self.url!r}, opened=False)'

    def read(self, size: int | None = None, /) -> bytes:
        """
        Read bytes from current position.

        Args:
            size: Number of bytes to read (-1 for all remaining)

        Returns:
            Bytes read from the file
        """
        return self._ohf.read(size)

    def prefetch(self, ranges: Iterable[tuple[int, int]]) -> int:
        """
        Warm the block cache for the given (offset, length) byte ranges.

        Adjacent and overlapping ranges are coalesced into as few range
        requests as possible and clamped to the file size. Later reads of
        warmed ranges are served from cache without touching the network.

        Args:
            ranges: Iterable of (offset, length) tuples to warm

        Returns:
            Number of bytes newly requested (0 if everything was cached)

        Raises:
            ValueError: If file is not opened
        """
        return self._ohf._cache.prefetch(ranges)

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """
        Change stream position.

        Args:
            offset: Byte offset
            whence: How to interpret offset (0=absolute, 1=relative, 2=from end)

        Returns:
            New absolute position
        """
        return self._ohf.seek(offset, whence)

    def tell(self) -> int:
        """
        Get current stream position.

        Returns:
            Current byte position in file
        """
        return self._ohf._position

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True
