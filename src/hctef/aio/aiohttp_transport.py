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
            headers = {'Range': 'bytes=0-'}
            async with self._session.get(url, headers=headers) as response:
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
            HctefNetworkError: If range request fails
        """
        try:
            headers = {'Range': f'bytes={start}-{end - 1}'}
            async with self._session.get(url, headers=headers) as response:
                return await response.read()
        except RuntimeError:
            raise
        except Exception as e:
            raise HctefNetworkError(
                f'Failed to fetch bytes {start}-{end} from {url}',
            ) from e

    async def close(self) -> None:
        """Close the aiohttp session."""
        await self._session.close()
