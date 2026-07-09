# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

### Added

### Changed

### Fixed

### Deprecated

### Removed

### Security

## [v0.3.2] - 2026-07-09

No breaking changes.

### Added

- `RangeRequestsUnsupportedError`, a subclass of `HctefNetworkError`, raised
  when a server cannot serve usable range requests. Its `reason` attribute is
  `'no-range-support'` (server answered 200 to a bounded Range request) or
  `'content-range-hidden'` (the Content-Range header was not visible,
  typically a missing `Access-Control-Expose-Headers` on CORS requests).
  Transient network failures and 5xx/429 responses are never classified as
  this error.
- All exceptions (`HctefError`, `HctefNetworkError`, `HctefUrlError`,
  `RangeRequestsUnsupportedError`) are now exported from the package root,
  and their class names are documented as stable public API.
- `prefetch(ranges)` on `AsyncHttpFile` and `HttpFile`: warm the block cache
  for a batch of `(offset, length)` ranges in one call. Adjacent and
  overlapping ranges are coalesced into as few range requests as possible;
  returns the number of bytes newly requested (0 when already cached).
- `max_concurrency` parameter on `AsyncHttpFile` (default `8`): caps the
  number of range requests in flight at once per file, protecting servers
  that fail range GETs under concurrent load (e.g. HTTP/2 single-connection
  browsers fanning out column-chunk reads).
- Retry with exponential backoff (up to 3 attempts) on 429/5xx range
  requests in both async transports, honoring `Retry-After` (seconds form).
  The pyfetch transport also retries raw fetch failures, since browsers mask
  CORS-less error responses as generic fetch errors.
- README "Stability Guarantees" section documenting transport
  auto-selection, block-cache read-through semantics, the
  no-context-manager reader lifecycle, and exception-name stability.

### Changed

- Exception message wording may change between releases; match on exception
  class names, which are now the documented contract.
- The aiohttp transport's previous single immediate retry on 500/502/503/504
  is replaced by the shared backoff policy above, which also covers 429.

### Fixed

- The pyfetch transport's `probe()` no longer misreports HTTP 4xx/5xx error
  responses as missing range support; they now raise a plain
  `HctefNetworkError` naming the status.

### Deprecated

### Removed

### Security

## [v0.3.1] - 2026-07-06

First tracked version!

[unreleased]: https://github.com/jkeifer/hctef/compare/v0.3.2...HEAD
[v0.3.2]: https://github.com/jkeifer/hctef/releases/tag/v0.3.2
[v0.3.1]: https://github.com/jkeifer/hctef/releases/tag/v0.3.1
