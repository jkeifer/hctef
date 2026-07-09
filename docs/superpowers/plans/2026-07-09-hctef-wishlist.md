# hctef API Wishlist Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the four ver-por-que client requests from `hctef-api-wishlist.md`: a stable exception taxonomy, retry-with-backoff on 5xx/429, a per-file concurrency cap, and a public `prefetch()` API — plus documented stability guarantees.

**Architecture:** All changes stay inside hctef's existing layering: exceptions in `src/hctef/exceptions.py` (shared by sync + async), retry logic in the transport layer (`src/hctef/aio/transport.py` helper used by both built-in transports), the concurrency cap in `_OpenedAsyncHttpFile._do_fetch_range` (the single choke point every transport-bound fetch passes through, including injected transports), and prefetch on `AsyncBlockCache` (reusing its existing block math/coalescing/in-flight dedup).

**Tech Stack:** Python ≥3.12, stdlib + aiohttp (optional extra), pytest + pytest-asyncio, uv, ruff, mypy.

## Global Constraints

- Python `>=3.12` (from pyproject); **zero new runtime dependencies** (`dependencies = []`).
- Exception **class names are public API**: the client greps traceback text across the pyodide/JS boundary. Never rename `HctefError`, `HctefNetworkError`, `HctefUrlError`, or the new `RangeRequestsUnsupportedError` once merged. Message wording stays free to change.
- Code style: single quotes (`[tool.ruff.format] quote-style = 'single'`), ruff lint rules per pyproject (note `COM` = trailing commas required, `RET`, `SIM` are on).
- Run tests with `uv run pytest` (coverage is on by default via `addopts="--cov=hctef"`). Baseline: 66 passed.
- Type-check with `uv run mypy src tests`.
- A 5xx/429 must **never** be classified as "range requests unsupported" — only a 200-to-a-bounded-Range or a hidden `Content-Range` may be.
- Deliberately skipped (YAGNI, note only): sync `HttpFile` `max_concurrency` (sync fetches are serial — there is no fan-out to cap), HTTP-date form of `Retry-After` (seconds form only), retry on `probe()` (wishlist only asks for range-request retry).

---

### Task 1: Exception taxonomy — `RangeRequestsUnsupportedError`

**Files:**
- Modify: `src/hctef/exceptions.py`
- Modify: `src/hctef/__init__.py`
- Modify: `src/hctef/aio/aiohttp_transport.py` (probe no-Content-Range site, `_get_range` 200 site)
- Modify: `src/hctef/aio/pyfetch_transport.py` (probe no-Content-Range site, `fetch_range` non-206 site)
- Modify: `src/hctef/http_file.py` (`_get_file_size` no-Content-Range site, `_fetch_range` non-206 site)
- Test: `tests/test_transport.py`, `tests/test_exceptions.py` (new)

**Interfaces:**
- Produces: `hctef.exceptions.RangeRequestsUnsupportedError(HctefNetworkError)` with constructor `(message: str, *, reason: RangeUnsupportedReason)` and attribute `reason`; `RangeUnsupportedReason = Literal['no-range-support', 'content-range-hidden']`. Re-exported from `hctef` root. Task 2 imports it in transports.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_exceptions.py`:

```python
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
```

In `tests/test_transport.py`, add to the imports from `hctef.exceptions`:

```python
from hctef.exceptions import HctefNetworkError, RangeRequestsUnsupportedError
```

Add these tests (near the other probe/fetch tests), and **update two existing tests**:

```python
@pytest.mark.asyncio
async def test_aiohttp_probe_no_range_support_typed() -> None:
    transport, _ = await _scripted_transport(_FakeAiohttpResponse(200, DATA))
    with pytest.raises(RangeRequestsUnsupportedError) as excinfo:
        await transport.probe(URL)
    assert excinfo.value.reason == 'no-range-support'


@pytest.mark.asyncio
async def test_pyfetch_probe_hidden_content_range_typed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 206 without Content-Range: the server honored the Range request but
    # CORS hid the header (missing Access-Control-Expose-Headers)
    async def pyfetch(url: str, **kwargs: Any) -> FakeResponse:
        return FakeResponse(206, {'content-type': 'application/octet-stream'})

    _install_fake_pyodide(monkeypatch, pyfetch)
    transport = create_transport('pyfetch')
    with pytest.raises(RangeRequestsUnsupportedError) as excinfo:
        await transport.probe(URL)
    assert excinfo.value.reason == 'content-range-hidden'


@pytest.mark.asyncio
async def test_pyfetch_probe_no_range_support_typed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 200 without Content-Range: the server ignored the Range header
    async def pyfetch(url: str, **kwargs: Any) -> FakeResponse:
        return FakeResponse(200, {}, DATA)

    _install_fake_pyodide(monkeypatch, pyfetch)
    transport = create_transport('pyfetch')
    with pytest.raises(RangeRequestsUnsupportedError) as excinfo:
        await transport.probe(URL)
    assert excinfo.value.reason == 'no-range-support'
```

Update `test_aiohttp_fetch_range_rejects_non_206` to assert the typed error:

```python
@pytest.mark.asyncio
async def test_aiohttp_fetch_range_rejects_non_206() -> None:
    # A 200 means the server ignored Range; the full body must not be
    # returned as if it were the slice, and it is not worth a retry.
    transport, session = await _scripted_transport(_FakeAiohttpResponse(200, DATA))
    with pytest.raises(RangeRequestsUnsupportedError) as excinfo:
        await transport.fetch_range(URL, 10, 20)
    assert excinfo.value.reason == 'no-range-support'
    assert len(session.requests) == 1
```

Update `test_pyfetch_fetch_range_non_206` the same way:

```python
@pytest.mark.asyncio
async def test_pyfetch_fetch_range_non_206(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def pyfetch(url: str, **kwargs: Any) -> FakeResponse:
        return FakeResponse(200, {}, DATA)

    _install_fake_pyodide(monkeypatch, pyfetch)
    transport = create_transport('pyfetch')
    with pytest.raises(RangeRequestsUnsupportedError) as excinfo:
        await transport.fetch_range(URL, 0, 10)
    assert excinfo.value.reason == 'no-range-support'
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_exceptions.py tests/test_transport.py -v --no-cov`
Expected: FAIL with `ImportError: cannot import name 'RangeRequestsUnsupportedError'`

- [ ] **Step 3: Implement**

Replace `src/hctef/exceptions.py` with:

```python
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
```

Replace `src/hctef/__init__.py` with:

```python
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
```

In `src/hctef/aio/aiohttp_transport.py`, change the exceptions import to:

```python
from hctef.exceptions import HctefNetworkError, RangeRequestsUnsupportedError
```

In `probe()`, replace the no-Content-Range raise:

```python
                # If no Content-Range header, server doesn't support ranges
                raise RangeRequestsUnsupportedError(
                    f'Server does not support range requests for {url}',
                    reason='no-range-support',
                )
```

In `_get_range()`, split the non-206 branch so a 200 is typed:

```python
            if response.status == 200:
                # The server ignored the Range header; its full body must
                # never be cached as if it were the slice.
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
```

In `src/hctef/aio/pyfetch_transport.py`, change the import:

```python
from hctef.exceptions import HctefNetworkError, RangeRequestsUnsupportedError
```

In `probe()`, replace the no-Content-Range raise (the 206/200 status split is what distinguishes the two reasons):

```python
            if response.status == 206:
                # Server honored the Range request but CORS hid the header
                raise RangeRequestsUnsupportedError(
                    f'Content-Range header is not visible for {url}; '
                    f'{_CORS_HINT}',
                    reason='content-range-hidden',
                )
            raise RangeRequestsUnsupportedError(
                f'Server does not support range requests for {url}, '
                f'or the Content-Range header is not visible; {_CORS_HINT}',
                reason='no-range-support',
            )
```

In `fetch_range()`, split the non-206 branch:

```python
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
```

In `src/hctef/http_file.py`, change the exceptions import to include the new class:

```python
from .exceptions import (
    HctefNetworkError,
    HctefUrlError,
    RangeRequestsUnsupportedError,
)
```

In `_get_file_size()`, replace the no-Content-Range raise:

```python
                # If no Content-Range header, server doesn't support ranges
                raise RangeRequestsUnsupportedError(
                    f'Server does not support range requests for '
                    f'{self.http_file.url}',
                    reason='no-range-support',
                )
```

In `_fetch_range()`, split the non-206 branch:

```python
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
```

Note: every `except HctefNetworkError: raise` pass-through in these files already re-raises the subclass unchanged — no changes needed there.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest -q`
Expected: all pass (66 baseline + new), including the untouched message-matching tests (`test_pyfetch_probe_cors_hidden_headers` still matches `Access-Control-Expose-Headers`, `test_aiohttp_probe_no_range_support_message_not_shadowed` still matches `does not support range requests`).

Run: `uv run mypy src tests`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add src/hctef/exceptions.py src/hctef/__init__.py src/hctef/aio/aiohttp_transport.py src/hctef/aio/pyfetch_transport.py src/hctef/http_file.py tests/test_exceptions.py tests/test_transport.py
git commit -m "feat: typed RangeRequestsUnsupportedError with reason attribute"
```

---

### Task 2: Retry with backoff on 5xx/429 in both async transports

**Files:**
- Modify: `src/hctef/aio/transport.py` (shared retry helper)
- Modify: `src/hctef/aio/aiohttp_transport.py` (use helper; add 429 + Retry-After; delete `_RetryableStatusError`/`_RETRYABLE_STATUSES`)
- Modify: `src/hctef/aio/pyfetch_transport.py` (add retry to `fetch_range`)
- Test: `tests/test_transport.py`

**Interfaces:**
- Consumes: `RangeRequestsUnsupportedError` from Task 1.
- Produces: in `hctef.aio.transport`: `RETRY_ATTEMPTS = 3`, `RETRY_BACKOFF_SECONDS = 0.5`, `RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})`, `class RetryableFetchError(Exception)` with `(message: str, retry_after: float | None = None)`, `def parse_retry_after(value: str | None) -> float | None`, `async def fetch_with_retries(attempt: Callable[[], Awaitable[bytes]]) -> bytes`. These are internal plumbing (not re-exported), but both transports import them.

- [ ] **Step 1: Write the failing tests**

In `tests/test_transport.py`, add near the top:

```python
import hctef.aio.transport as transport_mod
```

Add a no-backoff fixture (retry tests must not sleep 1.5s):

```python
@pytest.fixture
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(transport_mod, 'RETRY_BACKOFF_SECONDS', 0.0)
```

Update the two existing aiohttp retry tests for 3 total attempts and add the fixture:

```python
@pytest.mark.asyncio
@pytest.mark.parametrize(
    'failure',
    [
        pytest.param(aiohttp.ServerDisconnectedError(), id='disconnect'),
        pytest.param(aiohttp.ClientOSError(), id='conn-reset'),
        pytest.param(aiohttp.ClientPayloadError(), id='truncated-body'),
        pytest.param(_FakeAiohttpResponse(503), id='slowdown-503'),
        pytest.param(_FakeAiohttpResponse(429), id='too-many-requests'),
    ],
)
async def test_aiohttp_fetch_range_retries(
    failure: Outcome,
    no_backoff: None,
) -> None:
    transport, session = await _scripted_transport(
        failure,
        _FakeAiohttpResponse(206, DATA),
    )
    assert await transport.fetch_range(URL, 10, 20) == DATA
    assert len(session.requests) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'failure',
    [
        pytest.param(aiohttp.ServerDisconnectedError(), id='disconnect'),
        pytest.param(_FakeAiohttpResponse(503), id='slowdown-503'),
    ],
)
async def test_aiohttp_fetch_range_persistent_failure_fails(
    failure: Outcome,
    no_backoff: None,
) -> None:
    transport, session = await _scripted_transport(failure, failure, failure)
    with pytest.raises(HctefNetworkError, match='Failed to fetch'):
        await transport.fetch_range(URL, 10, 20)
    # bounded attempts, no runaway loop
    assert len(session.requests) == transport_mod.RETRY_ATTEMPTS
```

(This replaces `test_aiohttp_fetch_range_retries_once`; delete the old version.)

Add new tests:

```python
@pytest.mark.asyncio
async def test_retry_honors_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(transport_mod.asyncio, 'sleep', fake_sleep)
    transport, session = await _scripted_transport(
        _FakeAiohttpResponse(429, headers={'Retry-After': '2'}),
        _FakeAiohttpResponse(206, DATA),
    )
    assert await transport.fetch_range(URL, 10, 20) == DATA
    assert sleeps == [2.0]


def test_parse_retry_after() -> None:
    assert transport_mod.parse_retry_after('2') == 2.0
    assert transport_mod.parse_retry_after('0') == 0.0
    assert transport_mod.parse_retry_after(None) is None
    # HTTP-date form is unsupported: fall back to backoff
    assert (
        transport_mod.parse_retry_after('Wed, 21 Oct 2015 07:28:00 GMT') is None
    )


@pytest.mark.asyncio
async def test_pyfetch_fetch_range_retries_500(
    monkeypatch: pytest.MonkeyPatch,
    no_backoff: None,
) -> None:
    calls: list[int] = []

    async def pyfetch(url: str, **kwargs: Any) -> FakeResponse:
        calls.append(1)
        if len(calls) == 1:
            return FakeResponse(500, {})
        return FakeResponse(
            206,
            {'Content-Range': f'bytes 10-19/{len(DATA)}'},
            DATA[10:20],
        )

    _install_fake_pyodide(monkeypatch, pyfetch)
    transport = create_transport('pyfetch')
    assert await transport.fetch_range(URL, 10, 20) == DATA[10:20]
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_pyfetch_fetch_range_retries_masked_cors_failure(
    monkeypatch: pytest.MonkeyPatch,
    no_backoff: None,
) -> None:
    # Browsers mask CORS-less 5xx responses as generic fetch failures;
    # those must be retried, not treated as fatal on the first try.
    calls: list[int] = []

    async def pyfetch(url: str, **kwargs: Any) -> FakeResponse:
        calls.append(1)
        if len(calls) < 3:
            raise OSError('TypeError: Failed to fetch')
        return FakeResponse(
            206,
            {'Content-Range': f'bytes 10-19/{len(DATA)}'},
            DATA[10:20],
        )

    _install_fake_pyodide(monkeypatch, pyfetch)
    transport = create_transport('pyfetch')
    assert await transport.fetch_range(URL, 10, 20) == DATA[10:20]
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_pyfetch_fetch_range_persistent_500_fails(
    monkeypatch: pytest.MonkeyPatch,
    no_backoff: None,
) -> None:
    calls: list[int] = []

    async def pyfetch(url: str, **kwargs: Any) -> FakeResponse:
        calls.append(1)
        return FakeResponse(500, {})

    _install_fake_pyodide(monkeypatch, pyfetch)
    transport = create_transport('pyfetch')
    with pytest.raises(HctefNetworkError, match='Failed to fetch'):
        await transport.fetch_range(URL, 10, 20)
    assert len(calls) == transport_mod.RETRY_ATTEMPTS
```

`_FakeAiohttpResponse` already accepts a `headers` kwarg — no fake changes needed.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_transport.py -v --no-cov`
Expected: new tests FAIL (`AttributeError: ... has no attribute 'RETRY_BACKOFF_SECONDS'`, pyfetch retries not happening); pre-existing tests still pass.

- [ ] **Step 3: Implement**

In `src/hctef/aio/transport.py`, add after the imports (extend the existing imports with `asyncio` and `Awaitable, Callable` from `collections.abc`):

```python
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
```

In `src/hctef/aio/aiohttp_transport.py`:

Delete `_RETRYABLE_STATUSES` and `class _RetryableStatusError`. Keep `_RETRYABLE_ERRORS`. Update the transport import:

```python
from .transport import (
    RETRYABLE_STATUSES,
    RemoteFileInfo,
    RetryableFetchError,
    fetch_with_retries,
    parse_retry_after,
)
```

Replace `fetch_range` and `_get_range`:

```python
    async def fetch_range(self, url: str, start: int, end: int) -> bytes:
        """
        Fetch the byte range [start, end) using the aiohttp session.

        Transient failures (connection drops, 5xx, 429) are retried with
        backoff, honoring Retry-After when present.

        Args:
            url: URL to fetch from
            start: Start byte position (inclusive)
            end: End byte position (exclusive)

        Returns:
            Bytes fetched from the range

        Raises:
            RangeRequestsUnsupportedError: If the server ignored the Range
                header (responded 200)
            HctefNetworkError: If the range request fails after retries
        """
        headers = {'Range': f'bytes={start}-{end - 1}'}

        async def attempt() -> bytes:
            try:
                return await self._get_range(url, headers, start, end)
            except _RETRYABLE_ERRORS as e:
                raise RetryableFetchError(repr(e)) from e

        try:
            return await fetch_with_retries(attempt)
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
            if response.status in RETRYABLE_STATUSES:
                raise RetryableFetchError(
                    f'HTTP {response.status}',
                    retry_after=parse_retry_after(
                        response.headers.get('Retry-After'),
                    ),
                )
            if response.status == 200:
                # The server ignored the Range header; its full body must
                # never be cached as if it were the slice.
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
            return await response.read()
```

Also update the comment above `_RETRYABLE_ERRORS` (retries are no longer "one retry"):

```python
# Connection-level failures that surface when reusing a pooled keep-alive
# connection the server has since closed (e.g. S3 idle timeout), or when a
# transfer is cut off mid-body. Range GETs are idempotent, so retrying is
# safe: aiohttp discards the dead connection and the retry opens a fresh one.
```

In `src/hctef/aio/pyfetch_transport.py`, update the transport import:

```python
from .transport import (
    RETRYABLE_STATUSES,
    RemoteFileInfo,
    RetryableFetchError,
    fetch_with_retries,
    parse_retry_after,
)
```

Replace `fetch_range`:

```python
    async def fetch_range(self, url: str, start: int, end: int) -> bytes:
        """
        Fetch the byte range [start, end) using the browser fetch API.

        Transient failures are retried with backoff, honoring Retry-After
        when visible. Raw fetch failures are retried too: browsers mask
        CORS-less error responses (e.g. a 500 without CORS headers) as
        generic fetch failures, and this range GET is idempotent.

        Args:
            url: URL to fetch from
            start: Start byte position (inclusive)
            end: End byte position (exclusive)

        Returns:
            Bytes fetched from the range

        Raises:
            RangeRequestsUnsupportedError: If the server ignored the Range
                header (responded 200)
            HctefNetworkError: If the range request fails after retries
        """
        self._check_open()

        async def attempt() -> bytes:
            try:
                response = await self._pyfetch(
                    url,
                    headers={'Range': f'bytes={start}-{end - 1}'},
                )
            except Exception as e:
                raise RetryableFetchError(repr(e)) from e

            if response.status in RETRYABLE_STATUSES:
                headers = {k.lower(): v for k, v in response.headers.items()}
                raise RetryableFetchError(
                    f'HTTP {response.status}',
                    retry_after=parse_retry_after(headers.get('retry-after')),
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

        try:
            return await fetch_with_retries(attempt)
        except (RuntimeError, HctefNetworkError):
            raise
        except Exception as e:
            raise HctefNetworkError(
                f'Failed to fetch bytes {start}-{end} from {url}',
            ) from e
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest -q` — expected: all pass, and the suite stays fast (no real backoff sleeps).
Run: `uv run mypy src tests` — expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add src/hctef/aio/transport.py src/hctef/aio/aiohttp_transport.py src/hctef/aio/pyfetch_transport.py tests/test_transport.py
git commit -m "feat: retry 5xx/429 with backoff and Retry-After in async transports"
```

---

### Task 3: Concurrency cap on `AsyncHttpFile`

**Files:**
- Modify: `src/hctef/aio/async_http_file.py`
- Test: `tests/test_concurrency.py` (new)

**Interfaces:**
- Produces: `AsyncHttpFile(url, ..., max_concurrency: int = 8)`; `ValueError` when `max_concurrency < 1`. All transport fetches (reads and Task 4's prefetch) funnel through `_OpenedAsyncHttpFile._do_fetch_range`, which acquires `self._fetch_semaphore`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_concurrency.py`:

```python
"""
The per-file concurrency cap on range requests.

Real servers (e.g. data.source.coop) 500 a large fraction of range GETs
once too many are in flight on one HTTP/2 connection; hctef owns the
fan-out, so it owns the in-flight discipline.
"""

import asyncio

from typing import Any

import pytest

from hctef.aio import AsyncHttpFile

from .test_transport import DATA, URL, FakeTransport


class TrackingTransport(FakeTransport):
    """Records the maximum number of concurrently in-flight fetches."""

    def __init__(self) -> None:
        super().__init__(DATA)
        self.active = 0
        self.max_active = 0

    async def fetch_range(self, url: str, start: int, end: int) -> bytes:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        # Yield long enough for every queued fetch task to get a turn, so
        # an uncapped fan-out would be observed as high concurrency.
        await asyncio.sleep(0.01)
        self.active -= 1
        return await super().fetch_range(url, start, end)


@pytest.mark.asyncio
async def test_max_concurrency_caps_inflight_fetches(tmp_path: Any) -> None:
    transport = TrackingTransport()
    async with AsyncHttpFile(
        URL,
        transport=transport,
        block_size=64,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
        max_concurrency=2,
    ) as hf:
        # 16 distinct single-block reads dispatched at once
        results = await asyncio.gather(
            *(hf.cursor.ohf.read(i * 64, 64) for i in range(16)),
        )
    assert results == [DATA[i * 64 : (i + 1) * 64] for i in range(16)]
    assert len(transport.fetches) == 16
    assert transport.max_active <= 2


def test_max_concurrency_must_be_positive() -> None:
    with pytest.raises(ValueError, match='max_concurrency'):
        AsyncHttpFile(URL, max_concurrency=0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_concurrency.py -v --no-cov`
Expected: FAIL with `TypeError: ... unexpected keyword argument 'max_concurrency'`

- [ ] **Step 3: Implement**

In `src/hctef/aio/async_http_file.py`:

Add `import asyncio` at the top (with the other imports).

In `_OpenedAsyncHttpFile.__init__`, after `self.size = size`:

```python
        self._fetch_semaphore = asyncio.Semaphore(http_file._max_concurrency)
```

In `_do_fetch_range`, wrap the transport call:

```python
        # The cap deliberately covers retries/backoff too: a struggling
        # server shouldn't see extra pressure while we're backing off.
        async with self._fetch_semaphore:
            return await self.transport.fetch_range(
                self.http_file.url,
                start,
                end,
            )
```

In `AsyncHttpFile.__init__`, add the parameter after `immutable`:

```python
        immutable: bool | None = None,
        max_concurrency: int = 8,
        minimum_range_request_bytes: int | None = None,
```

Add to the docstring's Keyword Args (after `immutable`):

```
            max_concurrency:
                Maximum number of range requests in flight at once for
                this file. Browsers put every request to an origin on one
                connection, and real servers start failing range GETs
                past ~12-16 concurrent requests; the default of 8 stays
                comfortably under that while keeping the pipe full.
```

And in the body, after `_check_url(url)`:

```python
        if max_concurrency < 1:
            raise ValueError('max_concurrency must be >= 1')
```

And store it with the other attributes:

```python
        self._max_concurrency = max_concurrency
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest -q` — expected: all pass.
Run: `uv run mypy src tests` — expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add src/hctef/aio/async_http_file.py tests/test_concurrency.py
git commit -m "feat: max_concurrency cap on AsyncHttpFile range requests"
```

---

### Task 4: Public `prefetch()` on `AsyncHttpFile` and `HttpFile`

**Files:**
- Modify: `src/hctef/aio/async_block_cache.py` (extract `_ensure_blocks`, add `prefetch`)
- Modify: `src/hctef/block_cache.py` (same refactor on the sync `BlockCache`; loosen `_missing_blocks` annotation)
- Modify: `src/hctef/aio/async_http_file.py` (add `prefetch` passthrough)
- Modify: `src/hctef/http_file.py` (add `prefetch` passthrough)
- Test: `tests/test_prefetch.py`, `tests/test_block_cache.py`, `tests/test_http_file.py`

**Interfaces:**
- Consumes: `_fetch_semaphore` discipline from Task 3 (prefetch fan-out is capped automatically because `_fetch_run` -> `_do_fetch_range`).
- Produces: `async AsyncHttpFile.prefetch(ranges: Iterable[tuple[int, int]]) -> int`, `async AsyncBlockCache.prefetch(...) -> int`, and sync twins `HttpFile.prefetch(ranges: Iterable[tuple[int, int]]) -> int` / `BlockCache.prefetch(...) -> int` — each tuple is `(offset, length)`; returns bytes newly requested from the transport (0 when fully cached). Internal `_ensure_blocks(indices: Sequence[int]) -> int` on both cache classes (async on `AsyncBlockCache`, sync on `BlockCache`).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_prefetch.py` (extend the existing import from `.test_transport` to also pull `FakeTransport`):

```python
from .test_transport import (  # noqa: F401
    DATA,
    URL,
    FakeTransport,
    fake_pyfetch,
)
```

```python
@pytest.mark.asyncio
async def test_prefetch_warms_cache_and_coalesces(tmp_path: Any) -> None:
    transport = FakeTransport()
    async with AsyncHttpFile(
        URL,
        transport=transport,
        block_size=64,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
    ) as hf:
        # Two adjacent ranges plus one overlapping: one coalesced request
        fetched = await hf.prefetch([(0, 64), (64, 64), (32, 64)])
        assert fetched == 128
        assert transport.fetches == [(0, 128)]

        # Reads inside the warmed span are served from cache
        assert await hf.read(100) == DATA[:100]
        assert transport.fetches == [(0, 128)]

        # Prefetching already-cached ranges is a no-op
        assert await hf.prefetch([(0, 128)]) == 0
        assert transport.fetches == [(0, 128)]


@pytest.mark.asyncio
async def test_prefetch_disjoint_ranges_fetch_separately(
    tmp_path: Any,
) -> None:
    transport = FakeTransport()
    async with AsyncHttpFile(
        URL,
        transport=transport,
        block_size=64,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
    ) as hf:
        fetched = await hf.prefetch([(0, 10), (512, 10)])
        # Whole blocks are fetched, one run per disjoint region
        assert fetched == 128
        assert sorted(transport.fetches) == [(0, 64), (512, 576)]


@pytest.mark.asyncio
async def test_prefetch_clamps_to_file_size(tmp_path: Any) -> None:
    transport = FakeTransport()
    async with AsyncHttpFile(
        URL,
        transport=transport,
        block_size=64,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
    ) as hf:
        size = hf.size
        fetched = await hf.prefetch([(size - 10, 1000)])
        assert fetched == 64  # just the final block
        assert transport.fetches == [(size - 64, size)]

        # Tail read served from cache
        hf.seek(-10, 2)
        assert await hf.read() == DATA[-10:]
        assert transport.fetches == [(size - 64, size)]
```

Append to `tests/test_block_cache.py` (its existing fixtures: `CONTENT` is 1050 bytes, `make_cache` uses `block_size=100`, `FetchLog` records `(start, end)` fetches):

```python
def test_sync_prefetch_warms_cache_and_coalesces(tmp_path: Path) -> None:
    log: FetchLog = []
    cache = make_cache(tmp_path, log)
    # Two adjacent ranges plus one overlapping: one coalesced request
    fetched = cache.prefetch([(0, 100), (100, 100), (50, 100)])
    assert fetched == 300
    assert log == [(0, 300)]

    # Reads inside the warmed span are served from cache
    assert cache.read(0, 250) == CONTENT[:250]
    assert log == [(0, 300)]

    # Prefetching already-cached ranges is a no-op
    assert cache.prefetch([(0, 300)]) == 0
    assert log == [(0, 300)]


def test_sync_prefetch_clamps_and_disjoint_ranges(tmp_path: Path) -> None:
    log: FetchLog = []
    cache = make_cache(tmp_path, log)
    # Block 0, plus the final short block via a range far past EOF
    fetched = cache.prefetch([(0, 10), (1040, 1000)])
    assert fetched == 150  # 100-byte block 0 + 50-byte final block 10
    assert sorted(log) == [(0, 100), (1000, 1050)]

    # Warmed tail read is served from cache
    assert cache.read(1040, 1050) == CONTENT[1040:1050]
    assert sorted(log) == [(0, 100), (1000, 1050)]
```

Append to `tests/test_http_file.py` (its existing helpers: `scripted_urlopen` fixture, `FakeUrlopenResponse`, `_probe_ok()` which reports a 1050-byte file, `URL`):

```python
def test_prefetch_passthrough(
    scripted_urlopen: tuple[list[UrlopenOutcome], list[UrlopenCall]],
    tmp_path: Path,
) -> None:
    script, calls = scripted_urlopen
    body = bytes(i % 256 for i in range(1050))
    script.append(_probe_ok())
    script.append(
        FakeUrlopenResponse(
            206,
            {'Content-Range': 'bytes 0-199/1050'},
            body[:200],
        ),
    )
    with HttpFile(
        URL,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
        block_size=100,
    ) as hf:
        assert hf.prefetch([(0, 100), (100, 100)]) == 200
        # Warmed reads are served from cache: no further urlopen calls
        n_calls = len(calls)
        assert hf.read(200) == body[:200]
        assert len(calls) == n_calls


def test_prefetch_on_closed_file_raises() -> None:
    hf = HttpFile(URL)
    with pytest.raises(ValueError, match='closed file'):
        hf.prefetch([(0, 10)])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_prefetch.py tests/test_block_cache.py tests/test_http_file.py -v --no-cov`
Expected: new tests FAIL with `AttributeError: ... has no attribute 'prefetch'` (on `AsyncHttpFile`, `BlockCache`, and `HttpFile` respectively); all pre-existing tests still pass.

- [ ] **Step 3: Implement**

Replace `src/hctef/aio/async_block_cache.py` with:

```python
from __future__ import annotations

import asyncio

from collections.abc import Awaitable, Callable, Iterable, Sequence

from hctef.block_cache import _BlockStore


class AsyncBlockCache(_BlockStore):
    """
    Async disk-backed block cache.

    Reuses all block math, presence scanning, coalescing, atomic writes,
    assembly and eviction from _BlockStore; only fetching is awaited. Multiple
    concurrent reads of the same missing block share a single in-flight fetch
    task, so overlapping reads never duplicate a download.
    """

    def __init__(
        self,
        url: str,
        file_size: int,
        fetch: Callable[[int, int], Awaitable[bytes]],
        **kwargs: object,
    ) -> None:
        super().__init__(url, file_size, **kwargs)  # type: ignore[arg-type]
        self._fetch = fetch
        self._inflight: dict[int, asyncio.Task[None]] = {}

    async def read(self, start: int, end: int) -> bytes:
        if end > self.file_size:
            raise ValueError('Read request extends beyond the end of the file.')
        if end <= start:
            return b''

        indices = self._block_indices(start, end)
        await self._ensure_blocks(indices)
        self._touch(indices)
        return self._assemble(start, end)

    async def prefetch(self, ranges: Iterable[tuple[int, int]]) -> int:
        """
        Warm the cache for every (offset, length) range given.

        Block indices across all ranges are deduplicated and coalesced, so
        adjacent and overlapping ranges collapse into as few requests as
        possible. Ranges are clamped to the file size.

        Returns:
            Number of bytes newly requested from the transport (0 when
            everything was already cached or in flight)
        """
        indices: set[int] = set()
        for offset, length in ranges:
            start = max(offset, 0)
            end = min(offset + length, self.file_size)
            indices.update(self._block_indices(start, end))
        return await self._ensure_blocks(sorted(indices))

    async def _ensure_blocks(self, indices: Sequence[int]) -> int:
        """
        Fetch missing blocks and await every in-flight fetch covering
        `indices` (ours and peers'). Returns bytes newly requested.
        """
        # Coalesce blocks that are neither on disk nor already being fetched.
        needed = [i for i in self._missing_blocks(indices) if i not in self._inflight]
        requested = 0
        for first, last in self._coalesce(needed):
            fetch_start, fetch_end = self._run_byte_range(first, last)
            requested += fetch_end - fetch_start
            task = asyncio.ensure_future(self._fetch_run(first, last))
            for index in range(first, last + 1):
                self._inflight[index] = task

        pending = {self._inflight[i] for i in indices if i in self._inflight}
        for task in pending:
            await task
        return requested

    async def _fetch_run(self, first: int, last: int) -> None:
        try:
            fetch_start, fetch_end = self._run_byte_range(first, last)
            # Protect every block currently in flight for this URL from eviction.
            protected = set(self._inflight) | set(range(first, last + 1))
            data = self._check_fetched(
                fetch_start,
                fetch_end,
                await self._fetch(fetch_start, fetch_end),
            )
            self._write_run(first, data, protected)
        finally:
            for index in range(first, last + 1):
                self._inflight.pop(index, None)
```

In `src/hctef/block_cache.py`:

Change the collections import to:

```python
from collections.abc import Callable, Iterable, Sequence
```

Loosen `_missing_blocks` to accept any iterable (it only iterates):

```python
    def _missing_blocks(self, indices: Iterable[int]) -> list[int]:
        return [i for i in indices if not self._block_path(i).exists()]
```

Replace the sync `BlockCache.read` with the same read/`_ensure_blocks`/`prefetch` split as the async cache:

```python
    def read(self, start: int, end: int) -> bytes:
        if end > self.file_size:
            raise ValueError('Read request extends beyond the end of the file.')
        if end <= start:
            return b''

        indices = self._block_indices(start, end)
        self._ensure_blocks(indices)
        self._touch(indices)
        return self._assemble(start, end)

    def prefetch(self, ranges: Iterable[tuple[int, int]]) -> int:
        """
        Warm the cache for every (offset, length) range given.

        Block indices across all ranges are deduplicated and coalesced, so
        adjacent and overlapping ranges collapse into as few requests as
        possible. Ranges are clamped to the file size.

        Returns:
            Number of bytes newly requested (0 when everything was cached)
        """
        indices: set[int] = set()
        for offset, length in ranges:
            start = max(offset, 0)
            end = min(offset + length, self.file_size)
            indices.update(self._block_indices(start, end))
        return self._ensure_blocks(sorted(indices))

    def _ensure_blocks(self, indices: Sequence[int]) -> int:
        """Fetch any missing blocks; returns bytes newly requested."""
        protected: set[int] = set()
        requested = 0
        for first, last in self._coalesce(self._missing_blocks(indices)):
            fetch_start, fetch_end = self._run_byte_range(first, last)
            protected.update(range(first, last + 1))
            requested += fetch_end - fetch_start
            data = self._check_fetched(
                fetch_start,
                fetch_end,
                self._fetch(fetch_start, fetch_end),
            )
            self._write_run(first, data, protected)
        return requested
```

In `src/hctef/http_file.py`, add the import:

```python
from collections.abc import Iterable
```

Add to `HttpFile` (after `read()`):

```python
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
```

In `src/hctef/aio/async_http_file.py`, add the import:

```python
from collections.abc import Iterable
```

Add to `AsyncHttpFile` (after `clone()`):

```python
    async def prefetch(self, ranges: Iterable[tuple[int, int]]) -> int:
        """
        Warm the block cache for the given (offset, length) byte ranges.

        Adjacent and overlapping ranges are coalesced into as few range
        requests as possible, subject to max_concurrency, and clamped to
        the file size. Later reads of warmed ranges are served from cache
        without touching the network.

        Args:
            ranges: Iterable of (offset, length) tuples to warm

        Returns:
            Number of bytes newly requested (0 if everything was cached)

        Raises:
            ValueError: If file is not opened
        """
        return await self.cursor.ohf.cache.prefetch(ranges)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest -q` — expected: all pass (including the pre-existing read/coalescing tests, which now route through `_ensure_blocks`).
Run: `uv run mypy src tests` — expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add src/hctef/aio/async_block_cache.py src/hctef/block_cache.py src/hctef/aio/async_http_file.py src/hctef/http_file.py tests/test_prefetch.py tests/test_block_cache.py tests/test_http_file.py
git commit -m "feat: prefetch for batched cache warming on async and sync readers"
```

---

### Task 5: Documented guarantees + reader-lifecycle test

**Files:**
- Modify: `README.md`
- Test: `tests/test_async_http_file.py`

**Interfaces:**
- Consumes: everything above (documents it).
- Produces: a `## Stability Guarantees` README section the client can pin against; a regression test for the no-context-manager lifecycle the client relies on.

- [ ] **Step 1: Write the failing (missing) lifecycle test**

Add to `tests/test_async_http_file.py` (add imports as needed: `pytest`, `AsyncHttpFile`, and `from .test_transport import DATA, URL, FakeTransport`):

```python
@pytest.mark.asyncio
async def test_open_close_without_context_manager(tmp_path: Any) -> None:
    # The ver-por-que worker opens without a context manager (the reader
    # outlives the parse) and closes explicitly later; close() is async
    # and idempotent.
    transport = FakeTransport()
    hf = await AsyncHttpFile(
        URL,
        transport=transport,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
    ).open()
    assert await hf.read(10) == DATA[:10]
    await hf.close()
    await hf.close()  # second close is a no-op
    with pytest.raises(ValueError, match='closed file'):
        await hf.read(1)
```

- [ ] **Step 2: Run test to verify it passes (it pins existing behavior)**

Run: `uv run pytest tests/test_async_http_file.py -v --no-cov`
Expected: PASS — this is a regression pin, not new behavior. If it fails, the current behavior differs from what the client relies on: stop and investigate before editing anything.

- [ ] **Step 3: Update README**

Add a new top-level section immediately before `## Error Handling`:

```markdown
## Stability Guarantees

Downstream consumers may rely on the following; changing any of them is a
breaking change:

- **Exception class names are API.** `HctefError`, `HctefNetworkError`,
  `RangeRequestsUnsupportedError`, and `HctefUrlError` are stable, greppable
  names (consumers on the far side of a traceback, e.g. pyodide -> JS, can
  only match on them). Message wording is NOT stable; match on class names,
  never message text.
- **Transport auto-selection.** `AsyncHttpFile` defaults to the `pyfetch`
  transport under Pyodide/emscripten and `aiohttp` everywhere else.
- **Block-cache read-through.** Every read (and `prefetch`) goes through the
  block cache; a range fetched once is served from cache for the reader's
  lifetime and never re-fetched.
- **Reader lifecycle.** `await AsyncHttpFile(url).open()` without a context
  manager is supported; `close()` is async and idempotent.
```

In the `## Error Handling` section, document the taxonomy (adapt to the section's existing style; the key content):

```markdown
- `HctefError`: base class for all hctef errors
- `HctefNetworkError`: transport failure (DNS, timeout, exhausted retries on
  5xx/429) — transient; retrying the operation may succeed
- `RangeRequestsUnsupportedError` (subclass of `HctefNetworkError`): this
  server cannot serve usable range requests — permanent for the URL; fall
  back to downloading the whole file. Its `reason` attribute is
  `'no-range-support'` (server answered 200 to a bounded Range request) or
  `'content-range-hidden'` (the Content-Range header was not visible,
  typically a missing `Access-Control-Expose-Headers` on CORS requests)
- `HctefUrlError`: invalid URL

Transient 5xx/429 responses on range requests are retried a bounded number
of times with exponential backoff, honoring `Retry-After` (seconds form)
when present.
```

In `## Configuration Options`, add `max_concurrency` to the `AsyncHttpFile` options (matching the surrounding format):

```markdown
- `max_concurrency` (default `8`): maximum number of range requests in
  flight at once for this file. Browsers put every request to an origin on
  a single connection, and real servers start failing range GETs past
  ~12-16 concurrent requests.
```

In the `### Asynchronous Usage` quick-start section, add a prefetch example:

```markdown
#### Warming the cache with `prefetch`

Batch-fetch known byte ranges (e.g. every bloom-filter or page-index range
from a Parquet footer) ahead of time; adjacent and overlapping ranges are
coalesced into as few requests as possible:

​```python
fetched = await f.prefetch([(offset, length) for offset, length in ranges])
​```

Returns the number of bytes newly requested (0 if already cached). Later
reads of the warmed ranges are served from cache. The synchronous
`HttpFile` has the same method (ranges are fetched sequentially there).
```

(Remove the zero-width escapes around the inner code fence when inserting.)

- [ ] **Step 4: Verify everything**

Run: `uv run pytest -q` — expected: all pass.
Run: `uv run mypy src tests` — expected: no errors.
Run: `uv run prek run --all-files` — expected: hooks pass (fix any formatting fallout).

- [ ] **Step 5: Commit**

```bash
git add README.md tests/test_async_http_file.py
git commit -m "docs: stability guarantees, error taxonomy, prefetch and max_concurrency"
```

---

## Post-plan notes (not tasks)

- Cutting a release is a git tag (hatch-vcs); this is a minor bump (new API, no breaking changes) — e.g. `0.4.0`.
- After release, the client should: bump the wheel pin in `scripts/fetch-wheels.py`, switch `isHctefNetworkError`/`isIncrementalReadError` to match on the `RangeRequestsUnsupportedError` class name, replace `_prefetch_blooms` with one `await f.prefetch(...)` call, and run `pyodide-parquet.integration.test.ts`. That work lives in ver-por-que, not here.
- Skipped as YAGNI: sync `HttpFile` `max_concurrency` (sync fetches are serial — nothing to cap), HTTP-date `Retry-After` parsing, retry on `probe()`.
