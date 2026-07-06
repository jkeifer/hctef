from __future__ import annotations

from typing import Any

try:
    import aiohttp
except ImportError:
    raise ImportError(
        'Must install hctef with `[async]` extra to get necessary dependencies',
    ) from None

from hctef.exceptions import HctefNetworkError

from .transport import RemoteFileInfo

# Connection-level failures that surface when reusing a pooled keep-alive
# connection the server has since closed (e.g. S3 idle timeout), or when a
# transfer is cut off mid-body. Range GETs are idempotent, so one retry is
# safe: aiohttp discards the dead connection and the retry opens a fresh one.
_RETRYABLE_ERRORS = (
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientOSError,
    aiohttp.ClientPayloadError,
)

# Transient server-side failures (e.g. S3 500/503 SlowDown) also worth one
# retry before giving up.
_RETRYABLE_STATUSES = frozenset({500, 502, 503, 504})


class _RetryableStatusError(Exception):
    """Internal: a response status from _RETRYABLE_STATUSES."""


class AiohttpTransport:
    """
    Transport backed by an ``aiohttp.ClientSession``.

    This is the default transport on regular (CPython) runtimes. It
    conforms structurally to the ``AsyncTransport`` protocol.
    """

    def __init__(self, session_kwargs: dict[str, Any] | None = None) -> None:
        """
        Create the transport and its underlying aiohttp session.

        Must be called from within a running event loop.

        Args:
            session_kwargs: Keyword arguments for ``aiohttp.ClientSession``
        """
        self._session = aiohttp.ClientSession(**(session_kwargs or {}))

    async def probe(self, url: str) -> RemoteFileInfo:
        """
        Get total file size and validators using an HTTP range request.

        Args:
            url: URL to probe

        Returns:
            RemoteFileInfo with size, ETag, and Last-Modified

        Raises:
            HctefNetworkError: If size cannot be determined
        """
        try:
            # One-byte range: the Content-Range total is the same, and the
            # server never starts streaming the whole body just for a probe.
            headers = {'Range': 'bytes=0-0'}
            async with self._session.get(url, headers=headers) as response:
                if response.status >= 400:
                    raise HctefNetworkError(
                        f'HTTP {response.status} probing {url}',
                    )
                etag = response.headers.get('ETag')
                last_modified = response.headers.get('Last-Modified')
                content_range = response.headers.get('Content-Range')
                if content_range:
                    return RemoteFileInfo(
                        int(content_range.split('/')[-1]),
                        etag,
                        last_modified,
                    )

                # If no Content-Range header, server doesn't support ranges
                raise HctefNetworkError(
                    f'Server does not support range requests for {url}',
                )
        except (RuntimeError, HctefNetworkError):
            raise
        except Exception as e:
            raise HctefNetworkError(
                f'Cannot determine file size for {url}',
            ) from e

    async def fetch_range(self, url: str, start: int, end: int) -> bytes:
        """
        Fetch the byte range [start, end) using the aiohttp session.

        Args:
            url: URL to fetch from
            start: Start byte position (inclusive)
            end: End byte position (exclusive)

        Returns:
            Bytes fetched from the range

        Raises:
            HctefNetworkError: If the range request fails or the server does
                not respond with 206 Partial Content
        """
        headers = {'Range': f'bytes={start}-{end - 1}'}
        try:
            try:
                return await self._get_range(url, headers, start, end)
            except (*_RETRYABLE_ERRORS, _RetryableStatusError):
                return await self._get_range(url, headers, start, end)
        except (RuntimeError, HctefNetworkError):
            raise
        except Exception as e:
            raise HctefNetworkError(
                f'Failed to fetch bytes {start}-{end} from {url}',
            ) from e

    async def _get_range(
        self,
        url: str,
        headers: dict[str, str],
        start: int,
        end: int,
    ) -> bytes:
        async with self._session.get(url, headers=headers) as response:
            if response.status in _RETRYABLE_STATUSES:
                raise _RetryableStatusError(f'HTTP {response.status}')
            if response.status != 206:
                # A 200 here means the server ignored the Range header; its
                # full body must never be cached as if it were the slice.
                raise HctefNetworkError(
                    f'Expected 206 Partial Content fetching bytes '
                    f'{start}-{end} from {url}, got {response.status}',
                )
            return await response.read()

    async def close(self) -> None:
        """Close the aiohttp session."""
        await self._session.close()
