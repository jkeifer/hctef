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
- For async: `aiohttp>=3.13.0`

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

## Error Handling

`hctef` defines custom exceptions:

- `HctefError`: Base exception class
- `HctefNetworkError`: Raised for network-related errors (inherits from
  `IOError`)
- `HctefUrlError`: Raised for invalid URLs (inherits from `ValueError`)

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

# Setup pre-commit
pre-commit install

# Run tests
pytest

# Run all checks with pre-commit
pre-commit run --all-files
```

## Future Ideas

- Allow uncached "cursor" for reading a large file segment
- Optional integrity checks on cached blocks

## License

Apache License 2.0

## What is hctef?

It's the HTTP Client That Eats Files, obviously.
