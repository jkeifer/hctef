from __future__ import annotations

from hctef.exceptions import HctefNetworkError, RangeRequestsUnsupportedError

from .transport import RemoteFileInfo

_CORS_HINT = (
    'if this is a cross-origin request in a browser, the server must expose '
    'the Content-Range header via `Access-Control-Expose-Headers`'
)


class PyfetchTransport:
    """
    Transport backed by ``pyodide.http.pyfetch`` (the browser fetch API).

    It conforms structurally to the ``AsyncTransport`` protocol.

    This is the default transport when running under Pyodide/emscripten,
    where socket-based clients like aiohttp cannot work. pyfetch ships
    with the Pyodide runtime itself; it is not a PyPI dependency.
    """

    def __init__(self) -> None:
        """
        Create the transport.

        Raises:
            ImportError: If not running inside a Pyodide runtime
        """
        try:
            from pyodide.http import pyfetch
        except ImportError:
            raise ImportError(
                "The 'pyfetch' transport requires the Pyodide runtime "
                '(pyodide.http.pyfetch) and is only available when running '
                'in the browser via Pyodide/emscripten',
            ) from None

        self._pyfetch = pyfetch
        self._closed = False

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError('Session is closed')

    async def probe(self, url: str) -> RemoteFileInfo:
        """
        Get total file size and validators using an HTTP range request.

        Uses a one-byte range request (``bytes=0-0``) so the browser does
        not stream the whole file body just to read the headers.

        Args:
            url: URL to probe

        Returns:
            RemoteFileInfo with size, ETag, and Last-Modified

        Raises:
            HctefNetworkError: If size cannot be determined. In browsers,
                response headers on cross-origin requests are hidden unless
                the server exposes them via CORS, so a missing Content-Range
                header may mean missing `Access-Control-Expose-Headers`
                rather than missing range support.
        """
        self._check_open()
        try:
            response = await self._pyfetch(url, headers={'Range': 'bytes=0-0'})
            if response.status >= 400:
                # A 4xx/5xx (e.g. 429/500) is a plain HTTP failure and must
                # never be classified as "range requests unsupported"
                raise HctefNetworkError(
                    f'HTTP {response.status} probing {url}',
                )
            headers = {k.lower(): v for k, v in response.headers.items()}
            etag = headers.get('etag')
            last_modified = headers.get('last-modified')
            content_range = headers.get('content-range')
            if content_range:
                return RemoteFileInfo(
                    int(content_range.split('/')[-1]),
                    etag,
                    last_modified,
                )

            # No Content-Range header: either the server doesn't support
            # range requests, or CORS is hiding the header from us
            if response.status == 206:
                # Server honored the Range request but CORS hid the header
                raise RangeRequestsUnsupportedError(
                    f'Content-Range header is not visible for {url}; {_CORS_HINT}',
                    reason='content-range-hidden',
                )
            raise RangeRequestsUnsupportedError(
                f'Server does not support range requests for {url}, '
                f'or the Content-Range header is not visible; {_CORS_HINT}',
                reason='no-range-support',
            )
        except (RuntimeError, HctefNetworkError):
            raise
        except Exception as e:
            raise HctefNetworkError(
                f'Cannot determine file size for {url}; {_CORS_HINT}',
            ) from e

    async def fetch_range(self, url: str, start: int, end: int) -> bytes:
        """
        Fetch the byte range [start, end) using the browser fetch API.

        Args:
            url: URL to fetch from
            start: Start byte position (inclusive)
            end: End byte position (exclusive)

        Returns:
            Bytes fetched from the range

        Raises:
            HctefNetworkError: If range request fails or the server does
                not respond with 206 Partial Content
        """
        self._check_open()
        try:
            response = await self._pyfetch(
                url,
                headers={'Range': f'bytes={start}-{end - 1}'},
            )
            if response.status == 200:
                raise RangeRequestsUnsupportedError(
                    f'Server ignored the Range header fetching bytes '
                    f'{start}-{end} from {url} (got 200)',
                    reason='no-range-support',
                )
            if response.status != 206:
                raise HctefNetworkError(
                    f'Expected 206 Partial Content fetching bytes '
                    f'{start}-{end} from {url}, got {response.status}',
                )
            return bytes(await response.bytes())
        except (RuntimeError, HctefNetworkError):
            raise
        except Exception as e:
            raise HctefNetworkError(
                f'Failed to fetch bytes {start}-{end} from {url}',
            ) from e

    async def close(self) -> None:
        """Mark the transport closed; pyfetch holds no persistent session."""
        self._closed = True
