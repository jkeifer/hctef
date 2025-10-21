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
- **Intelligent Caching**: Uses an interval tree to track cached byte ranges
  and minimize redundant requests
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
    minimum_range_request_bytes=8192,  # Minimum bytes per request (default: 8KB)
    prefetch_bytes=1048576,             # Bytes to prefetch on open (default: 1MB)
    prefetch_direction='END'            # 'START' or 'END' (default: 'END')
)
```

- **`minimum_range_request_bytes`**: The minimum number of bytes to request in
  a single HTTP Range request (except when filling small cache gaps)
- **`prefetch_bytes`**: How many bytes to fetch immediately when opening the
  file. Set to 0 to disable prefetching
- **`prefetch_direction`**: Whether to prefetch from the start (`'START'`) or
  end (`'END'`) of the file

## Requirements

- Python 3.12 or higher
- HTTP server must support Range requests
- For async: `aiohttp>=3.13.0`

## How It Works

When you open an HTTP file, `hctef`:

1. Sends an initial Range request to determine the file size and verify Range
   support
1. Optionally prefetches data from the start or end of the file
1. Maintains an in-memory cache of fetched byte ranges (not suitable for
   downloading complete large files)
1. On `read()`, checks the cache first and only fetches missing data from the
   server
1. Combines multiple small requests into larger ones based on
   `minimum_range_request_bytes`

This approach minimizes HTTP requests while providing efficient random access
to remote files.

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

- Consoldiate sync/async implementations
- Allow uncached "cursor" for reading a large file segement
- Cursors with separate caches (to allow clearing memory when done)
  - would allow cursor-based access with non-async implementation

## License

Apache License 2.0

## What is hctef?

It's the HTTP Client That Eats Files, obviously.
