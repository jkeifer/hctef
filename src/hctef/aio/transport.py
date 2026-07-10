from __future__ import annotations

import asyncio
import sys

from collections.abc import Awaitable, Callable
from typing import Any, Literal, NamedTuple, Protocol

TransportName = Literal['aiohttp', 'pyfetch']

# Retry policy shared by the built-in transports. Range GETs are
# idempotent, so retrying transient failures (connection drops, 5xx/429)
# is always safe.
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 0.5
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class RetryableFetchError(Exception):
    """
    Internal: a transient fetch failure worth retrying.

    Transports raise this from inside a fetch attempt to request a retry;
    it never escapes fetch_with_retries' callers unwrapped (the transport
    wraps the final failure in HctefNetworkError).
    """

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header value into seconds."""
    # ponytail: seconds form only; the HTTP-date form falls back to backoff
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


async def fetch_with_retries(attempt: Callable[[], Awaitable[bytes]]) -> bytes:
    """
    Run a fetch attempt, retrying RetryableFetchError with backoff.

    Sleeps retry_after when the failure carries one (from a Retry-After
    header), else an exponential backoff. Re-raises the final failure for
    the transport to wrap.
    """
    for tries in range(RETRY_ATTEMPTS):
        try:
            return await attempt()
        except RetryableFetchError as e:
            if tries == RETRY_ATTEMPTS - 1:
                raise
            delay = e.retry_after
            if delay is None:
                delay = RETRY_BACKOFF_SECONDS * 2**tries
            await asyncio.sleep(delay)
    raise AssertionError('unreachable')


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
