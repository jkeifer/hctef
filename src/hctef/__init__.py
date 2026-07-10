from .exceptions import (
    HctefError,
    HctefNetworkError,
    HctefUrlError,
    RangeRequestsUnsupportedError,
)
from .http_file import HttpFile

try:
    from .__version__ import __version__, __version_tuple__
except ImportError:
    __version__ = '0.0.0'
    __version_tuple__ = ('0', '0', '0')

__all__: list[str] = [
    'HctefError',
    'HctefNetworkError',
    'HctefUrlError',
    'HttpFile',
    'RangeRequestsUnsupportedError',
    '__version__',
    '__version_tuple__',
]
