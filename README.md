[![Tests](https://github.com/jkeifer/hctef/actions/workflows/ci.yml/badge.svg)](https://github.com/jkeifer/hctef/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/hctef.svg)](https://badge.fury.io/py/hctef)

# hctef

Python library with helper classes to read files over HTTP using Range
requests, with caching.

## Overview

`hctef` provides a file-like interface for reading files over HTTP/HTTPS, using
HTTP Range requests to fetch only the data you need. It includes intelligent
caching to minimize network requests and supports both synchronous and
asynchronous operations.

## Features

- **File-like API**: Works like a regular Python file object with `read()`,
  `seek()`, and `tell()` methods
- **Efficient Range Requests**: Fetches only the data you need using HTTP Range
  headers
- **Disk-backed block cache**: Caches fixed-size blocks on disk and leans on the
  OS page cache for the in-memory tier, so no file data is held in Python beyond
  transient buffers. The cache persists across processes and survives restarts
- **Prefetching**: Optionally prefetch data from the start or end of the file
- **Sync and Async**: Both synchronous and asynchronous implementations
  available
- **Context Manager Support**: Use with `with` statements for automatic cleanup

## Installation

```bash
pip install hctef
```

To include async support:

```bash
pip install hctef[async]
```

The `[async]` extra pulls in `aiohttp`, the default async transport on regular
(CPython) runtimes. When running under [Pyodide](https://pyodide.org)
(Python-in-the-browser via WebAssembly), no extra is needed: `AsyncHttpFile`
automatically uses `pyodide.http.pyfetch` (a thin wrapper over the browser's
`fetch` API that ships with the Pyodide runtime) instead, since socket-based
clients like aiohttp cannot work in the browser.

## Quick Start

### Synchronous Usage

```python
from hctef import HttpFile

url = "https://example.com/large-file.bin"

with HttpFile(url) as f:
    # Read first 100 bytes
    data = f.read(100)

    # Seek to a specific position
    f.seek(1000)

    # Read from current position
    more_data = f.read(50)

    # Get current position
    position = f.tell()

    # Seek relative to end of file
    f.seek(-100, 2)
```

### Asynchronous Usage

The async implementation supports independent cursors for concurrent reads:

```python
import asyncio
from hctef.aio import AsyncHttpFile

url = "https://example.com/large-file.bin"

async with AsyncHttpFile(url) as f:
    # Read first 100 bytes
    data = await f.read(100)

    # Seek to a specific position (synchronous - no I/O)
    f.seek(1000)

    # Read from current position
    more_data = await f.read(50)
```

#### Warming the cache with `prefetch`

Batch-fetch known byte ranges (e.g. every bloom-filter or page-index range
from a Parquet footer) ahead of time; adjacent and overlapping ranges are
coalesced into as few requests as possible:

```python
fetched = await f.prefetch([(offset, length) for offset, length in ranges])
```

Returns the number of bytes newly requested (0 if already cached). Later
reads of the warmed ranges are served from cache. The synchronous
`HttpFile` has the same method (ranges are fetched sequentially there).

#### Parallel Reads with Multiple Cursors

Create independent cursors to read from different positions concurrently:

```python
import asyncio
from hctef.aio import AsyncHttpFile

url = "https://example.com/large-file.bin"

async with AsyncHttpFile(url) as f:
    # Create independent cursors for parallel reading
    cursor1 = f.clone()
    cursor2 = f.clone()

    # Position each cursor at different locations
    f.seek(0)
    cursor1.seek(1000)
    cursor2.seek(2000)

    # Read from all three positions in parallel
    # All cursors share the same cache and HTTP session
    results = await asyncio.gather(
        f.read(100),        # Read bytes 0-100
        cursor1.read(100),  # Read bytes 1000-1100
        cursor2.read(100),  # Read bytes 2000-2100
    )

    # Each cursor maintains independent position
    print(f.tell())        # 100
    print(cursor1.tell())  # 1100
    print(cursor2.tell())  # 2100
```

Cursors are lightweight and share:

- HTTP session (connection pooling)
- Byte range cache (deduplication of overlapping requests)
- File metadata

#### Async Transports

`AsyncHttpFile` supports two HTTP transports, selected via the `transport`
parameter:

- **`'aiohttp'`** (default on CPython): uses an `aiohttp.ClientSession`.
  Requires the `[async]` extra. The `session_kwargs` parameter is passed
  through to `aiohttp.ClientSession` and is specific to this transport.
- **`'pyfetch'`** (default under Pyodide/emscripten): uses
  `pyodide.http.pyfetch`, i.e. the browser's `fetch` API. Requires no extra
  dependencies but only works inside a Pyodide runtime.

When `transport` is not given, the transport is chosen automatically based on
the runtime (`sys.platform == 'emscripten'` selects `'pyfetch'`):

```python
# Force a specific transport
f = AsyncHttpFile(url, transport='pyfetch')
```

##### Custom transports

`transport` also accepts a transport *instance*, used as-is. `AsyncTransport`
is a structural protocol (`typing.Protocol`), so any class with the three
methods works — no hctef imports or subclassing required:

```python
class MyTransport:
    async def probe(self, url: str) -> RemoteFileInfo:
        """Return an object with .size, .etag, and .last_modified."""

    async def fetch_range(self, url: str, start: int, end: int) -> bytes:
        """Return the byte range [start, end) — end-exclusive."""

    async def close(self) -> None:
        """Release any resources held by the transport."""


transport = MyTransport()
async with AsyncHttpFile(url, transport=transport) as f:
    data = await f.read(1024)
await transport.close()  # you own it; hctef won't close it for you
```

**Ownership rule:** transports created *by* `AsyncHttpFile` from a name
(`'aiohttp'`/`'pyfetch'`/auto-selected) are owned by it and closed on
`close()` (and on open failure). An *injected* instance is never closed by
`AsyncHttpFile` — the caller manages its lifecycle, which also makes it safe
to share one transport across several files. `session_kwargs` only configures
the built-in `'aiohttp'` backend; combining it with an injected instance (or
`'pyfetch'`) raises `ValueError`.

> **CORS note for the browser:** with the `'pyfetch'` transport, response
> headers on cross-origin requests are only visible when the server exposes
> them. If opening a file fails with a "cannot determine file size" error,
> the server likely needs to send
> `Access-Control-Expose-Headers: Content-Range` in addition to allowing the
> origin.

## Configuration Options

Both `HttpFile` and `AsyncHttpFile` accept the following parameters:

```python
HttpFile(
    url,
    prefetch_bytes=1048576,   # Bytes to prefetch on open (default: 1 MiB)
    prefetch_direction='END', # 'START' or 'END' (default: 'END')
    cache_dir=None,           # Where to store the block cache (default: temp dir)
    block_size=None,          # Fixed block size in bytes (default: 1 MiB)
    max_bytes=None,           # Optional cap on the whole cache dir (LRU eviction)
    immutable=None,           # Skip etag/last-modified validation
)
```

- **`prefetch_bytes`**: How many bytes to fetch immediately when opening the
  file. Set to 0 to disable prefetching
- **`prefetch_direction`**: Whether to prefetch from the start (`'START'`) or
  end (`'END'`) of the file
- **`cache_dir`**: Directory holding the disk block cache. When omitted, a
  per-process temporary directory is created and removed on close
- **`block_size`**: Fixed cache block size. All reads are serviced by fetching
  and storing whole blocks; this subsumes the old request-coalescing knob
- **`max_bytes`**: Optional size cap over the entire `cache_dir`. When exceeded,
  least-recently-used blocks are evicted at write time
- **`immutable`**: Trust an existing cache without revalidating `ETag` /
  `Last-Modified` against the live response

`AsyncHttpFile` additionally accepts:

- **`transport`**: Which async HTTP transport to use, `'aiohttp'` or
  `'pyfetch'` (see [Async Transports](#async-transports)). Defaults to
  `'pyfetch'` under Pyodide/emscripten and `'aiohttp'` everywhere else.
  May also be an `AsyncTransport` instance, which is used as-is and never
  closed by `AsyncHttpFile` — the caller owns its lifecycle
- **`session_kwargs`**: Keyword arguments for `aiohttp.ClientSession`;
  specific to the built-in `'aiohttp'` transport (raises `ValueError` when
  combined with `'pyfetch'` or an injected transport instance)
- **`max_concurrency`** (default `8`): maximum number of range requests in
  flight at once for this file. Browsers put every request to an origin on
  a single connection, and real servers start failing range GETs past
  ~12-16 concurrent requests

> **Note:** `minimum_range_request_bytes` is deprecated and ignored (it emits a
> `DeprecationWarning`); `block_size` replaces it.

### Environment variables

When the corresponding constructor argument is not given, configuration falls
back to these environment variables:

| Variable | Meaning |
| --- | --- |
| `HCTEF_CACHE_DIR` | Cache directory (else a temp dir is used) |
| `HCTEF_CACHE_BLOCK_BYTES` | Block size in bytes (default 1 MiB) |
| `HCTEF_CACHE_MAX_BYTES` | Cap for the whole cache dir (default: unbounded) |
| `HCTEF_CACHE_IMMUTABLE` | Truthy value (`1`/`true`/`yes`/`on`) to skip validation |

Precedence is: explicit constructor argument, then environment variable, then
the built-in default.

> **tmpfs caveat:** On Linux the default temporary directory (`/tmp`) is often a
> `tmpfs` mount backed by RAM. In that case the "disk" block cache actually
> lives in memory, defeating the goal of keeping bytes out of RAM. Set
> `cache_dir` / `HCTEF_CACHE_DIR` to a path on real disk when that matters.

## Requirements

- Python 3.12 or higher
- HTTP server must support Range requests
- For async on CPython: `aiohttp>=3.13.0` (the `[async]` extra)
- For async in the browser: the Pyodide runtime (provides
  `pyodide.http.pyfetch`; no extra packages needed)

## How It Works

When you open an HTTP file, `hctef`:

1. Sends an initial Range request to determine the file size and verify Range
   support, capturing `ETag` / `Last-Modified` validators
1. Opens (or validates and, on mismatch, wipes) a per-URL directory under the
   cache dir, keyed by `sha256(url)`, holding a `meta.json` and one file per
   fixed-size block
1. Optionally prefetches data from the start or end of the file
1. On `read()`, maps the request to a block range, fetches only the missing
   blocks (coalescing contiguous gaps into single Range requests), writes each
   block atomically, and assembles the result by reading the block files

Because blocks live on disk and are read back through the OS page cache, hot
data stays fast without being pinned in Python memory, and the cache is reused
by later opens and other processes sharing the same directory.

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

## Error Handling

`hctef` defines custom exceptions:

- `HctefError`: base class for all hctef errors
- `HctefNetworkError`: transport failure (DNS, timeout, exhausted retries on
  5xx/429) — transient; retrying the operation may succeed
- `RangeRequestsUnsupportedError` (subclass of `HctefNetworkError`): this
  server cannot serve usable range requests — permanent for the URL; fall
  back to downloading the whole file. Its `reason` attribute is
  `'no-range-support'` (server answered 200 to a bounded Range request) or
  `'content-range-hidden'` (the Content-Range header was not visible,
  typically a missing `Access-Control-Expose-Headers` on CORS requests)
- `HctefUrlError`: invalid URL (inherits from `ValueError`)

On the async transports (`AsyncHttpFile`), transient 5xx/429 responses on
range requests are retried a bounded number of times with exponential backoff,
honoring `Retry-After` (seconds form) when present; the synchronous `HttpFile`
does not retry.

```python
from hctef import HttpFile
from hctef.exceptions import HctefNetworkError, HctefUrlError

try:
    with HttpFile("https://example.com/file.bin") as f:
        data = f.read(100)
except HctefNetworkError as e:
    print(f"Network error: {e}")
except HctefUrlError as e:
    print(f"Invalid URL: {e}")
```

## Development

To set up for development:

```bash
# Clone the repository
git clone https://github.com/jkeifer/hctef
cd hctef

# Install dependencies
uv sync --all-extras --dev

# Setup git hooks (prek, a fast pre-commit reimplementation)
uv run prek install

# Run tests
uv run pytest

# Run all checks with prek
uv run prek run --all-files
```

## Future Ideas

- Allow uncached "cursor" for reading a large file segment
- Optional integrity checks on cached blocks

## License

Apache License 2.0

## What is hctef?

It's the HTTP Client That Eats Files, obviously.
