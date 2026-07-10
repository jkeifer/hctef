from hctef.exceptions import (
    HctefNetworkError,
    RangeRequestsUnsupportedError,
)


def test_range_unsupported_is_network_error() -> None:
    # The client's existing isinstance/name checks on HctefNetworkError
    # must keep matching the new subclass.
    exc = RangeRequestsUnsupportedError('nope', reason='no-range-support')
    assert isinstance(exc, HctefNetworkError)
    assert isinstance(exc, IOError)
    assert exc.reason == 'no-range-support'


def test_exceptions_reexported_from_root() -> None:
    import hctef

    assert hctef.RangeRequestsUnsupportedError is RangeRequestsUnsupportedError
    assert hctef.HctefNetworkError is HctefNetworkError
