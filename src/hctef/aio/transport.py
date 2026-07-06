from __future__ import annotations

import sys

from typing import Any, Literal, NamedTuple, Protocol

TransportName = Literal['aiohttp', 'pyfetch']


class RemoteFileInfo(NamedTuple):
    """File metadata discovered by probing a URL."""

    size: int
    etag: str | None
    last_modified: str | None


class AsyncTransport(Protocol):
    """
    Minimal async HTTP transport interface used by AsyncHttpFile.

    A transport knows how to probe a URL for its total size (verifying
    range-request support along the way), fetch byte ranges, and release
    any resources it holds.

    This is a structural protocol (``typing.Protocol``): any object with
    these three methods conforms, without subclassing or importing anything
    from hctef. Instances can be passed directly to ``AsyncHttpFile`` via
    its ``transport`` parameter; injected instances are owned by the caller
    and are never closed by ``AsyncHttpFile``.
    """

    async def probe(self, url: str) -> RemoteFileInfo:
        """
        Determine total file size and validators via an HTTP range request.

        Args:
            url: URL to probe

        Returns:
            RemoteFileInfo with size, ETag, and Last-Modified

        Raises:
            HctefNetworkError: If size or range support cannot be determined
        """

    async def fetch_range(self, url: str, start: int, end: int) -> bytes:
        """
        Fetch the byte range [start, end) from url.

        Args:
            url: URL to fetch from
            start: Start byte position (inclusive)
            end: End byte position (exclusive)

        Returns:
            Bytes fetched from the range

        Raises:
            HctefNetworkError: If the range request fails
        """

    async def close(self) -> None:
        """Release any resources held by the transport."""


def default_transport_name() -> TransportName:
    """
    Pick the default transport for the current runtime.

    Returns:
        'pyfetch' when running under Pyodide/emscripten, else 'aiohttp'
    """
    return 'pyfetch' if sys.platform == 'emscripten' else 'aiohttp'


def create_transport(
    transport: TransportName | None = None,
    session_kwargs: dict[str, Any] | None = None,
) -> AsyncTransport:
    """
    Instantiate the requested (or default) transport.

    Args:
        transport:
            Transport name, or None to auto-select based on the runtime
            (``default_transport_name()``).
        session_kwargs:
            Keyword arguments for ``aiohttp.ClientSession``. Only supported
            by the 'aiohttp' transport.

    Returns:
        A ready-to-use AsyncTransport instance

    Raises:
        ImportError: If the selected transport's backing library is missing
        ValueError: If the transport name is unknown, or session_kwargs is
            given with a non-aiohttp transport
    """
    name = transport if transport is not None else default_transport_name()

    if name == 'aiohttp':
        from .aiohttp_transport import AiohttpTransport

        return AiohttpTransport(session_kwargs)

    if name == 'pyfetch':
        if session_kwargs:
            raise ValueError(
                'session_kwargs is aiohttp-specific and not supported '
                "by the 'pyfetch' transport",
            )

        from .pyfetch_transport import PyfetchTransport

        return PyfetchTransport()

    raise ValueError(f'Unknown transport: {name!r}')
