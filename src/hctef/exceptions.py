class HctefError(Exception):
    """Base exception for all hctef errors."""


class HctefNetworkError(HctefError, IOError):
    """Network-related error while fetching data."""


class HctefUrlError(HctefError, ValueError):
    """Invalid URL for file."""
