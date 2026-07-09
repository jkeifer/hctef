from __future__ import annotations

from typing import Literal

RangeUnsupportedReason = Literal['no-range-support', 'content-range-hidden']


class HctefError(Exception):
    """Base exception for all hctef errors."""


class HctefNetworkError(HctefError, IOError):
    """Network-related error while fetching data."""


class RangeRequestsUnsupportedError(HctefNetworkError):
    """
    The server cannot serve usable range requests for this URL.

    Raised when a server answers 200 to a bounded Range request (no range
    support), or when the response looks like a range response but the
    Content-Range header is not visible (in browsers: a missing
    `Access-Control-Expose-Headers`).

    Class names in this module are public API: consumers on the far side
    of a traceback (e.g. pyodide -> JS) match on them, so they must stay
    stable even as message wording changes.

    Attributes:
        reason: 'no-range-support' when the server ignored the Range
            header; 'content-range-hidden' when the server honored it but
            the Content-Range header was hidden from us.
    """

    def __init__(self, message: str, *, reason: RangeUnsupportedReason) -> None:
        super().__init__(message)
        self.reason: RangeUnsupportedReason = reason


class HctefUrlError(HctefError, ValueError):
    """Invalid URL for file."""
