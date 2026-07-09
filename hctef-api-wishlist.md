# hctef APIs ver-por-que leans on

Companion to `por-que-internal-api.md`, split out because these asks belong to
hctef even though the worker reaches hctef through por-que (`AsyncHttpFile` is
imported via `from por_que import AsyncHttpFile`). Pinned wheel at time of
writing: **hctef 0.3.1**.

Same caveat as the por-que doc: none of this is version-guarded beyond the
wheel pin in `scripts/fetch-wheels.py` and the integration suite. **On an hctef
bump, run `pyodide-parquet.integration.test.ts`** — its range/no-range fixture
servers exercise both transports.

## 1. Error taxonomy: exception *names in message text* used as an API

The app's whole degraded-mode UX keys off recognizing "this server won't serve
range requests" — and today that recognition is string matching on message
prose, twice, on the far side of two boundaries:

| Where | What it matches | Why it's fragile |
| --- | --- | --- |
| `src/js/worker/pyodide-parquet.ts` `isHctefNetworkError` | `'HctefNetworkError'` in the JS `Error.message` (pyodide embeds the python traceback) | matches the class *name in a traceback*; any exception rename/wrap breaks the whole-file-download fallback |
| `src/components/info-panel/recovery.ts` `isIncrementalReadError` | `'HctefNetworkError'`, `'does not support HTTP range'`, `'range request'` | same, plus it depends on hctef's exact message wording; a reworded message turns the "Download full file" recovery button into a raw traceback |

The failure modes being conflated under one name today:

- server answers 200 to a bounded `Range` request (no range support)
- CORS hides `Content-Range` (missing `Access-Control-Expose-Headers`)
- garden-variety network failure (DNS, timeout, 5xx)

Only the first two should trigger the "download the whole file instead"
fallback; a transient network error should stay retryable. The single
`HctefNetworkError` name can't make that distinction.

**Cleaner public API:** a small exception hierarchy with stable names, e.g.
`RangeRequestsUnsupportedError(HctefNetworkError)` (covering both the 200-to-a-
Range-request case and the hidden-Content-Range case, ideally with a `reason`
attribute distinguishing them) vs. plain `HctefNetworkError` for transport
failures. The names are the contract: across pyodide, the traceback text is all
a JS consumer gets, so the class names themselves must be stable and greppable.
Message wording should then be free to change.

## 2. Cache warming: no public prefetch, so we fake one

`_prefetch_blooms` (in `PARSE_PY`) warms the block cache for every bloom filter
in the file by constructing `BloomFilter.from_reader(...)` per filter **and
discarding the result** — the only public way to pull a byte range through
hctef's block cache is to make something read it. That's one full filter parse
per warm (130 objects on the weather fixture) just to populate a cache.

**Cleaner public API:** an explicit range prefetch on the reader, e.g.
`await f.prefetch([(offset, length), ...])` (batched, coalescing adjacent
ranges into single requests), returning how many bytes/ranges were fetched.
ver-por-que would call it once with every `bloom_filter_offset/length` from the
footer. This also unlocks warming page-index ranges the same way later.

## 3. Concurrency cap (and 5xx backoff) on range requests

Real-world failure, reproduced 2026-07-09 (see
`source-coop-range-500-issue.md`): `data.source.coop` returns **500** to a
large fraction of range GETs once more than ~12–16 are simultaneously in
flight on one HTTP/2 connection — and in a browser, one connection per origin
is exactly what every request gets. Parsing the 82 MB CDL sample fans out one
read per column chunk (14 row groups × 3 columns); Safari dispatches the burst
as-is and ~25 % of the reads 500. Chrome only survives because its fetch
scheduler happens to pace requests below the threshold — luck, not a contract.
Worse, the 500 responses lack CORS headers, so the browser masks them as CORS
errors, and (per §1) a 5xx is conflated with "no range support" today.

**Cleaner public API:** hctef should own in-flight discipline, since it owns
the fan-out:

- a concurrency cap on outstanding range requests per `AsyncHttpFile`
  (semaphore, default ~8 — comfortably under the observed threshold, wide
  enough to keep the pipe full), e.g. `AsyncHttpFile(url, max_concurrency=8)`;
- retry with backoff on 5xx/429 (a couple of attempts, honoring
  `Retry-After`), so a transient server wobble doesn't abort a parse that
  ranges would otherwise finish.

Both belong below por-que: no caller placing individual reads can see the
global in-flight count, and every consumer of the pyfetch transport hits the
same class of server.

## 4. Behaviors relied on that deserve a documented guarantee

Not bugs — things that work today and would be easy to break without noticing:

- **Transport auto-selection:** `AsyncHttpFile` picks the pyfetch transport
  under emscripten (browser fetch in the worker) and node's global fetch under
  vitest. Both paths are load-bearing (app and integration suite respectively).
- **Block-cache read-through:** reads go through the block cache, and a range
  fetched once stays cached for the reader's lifetime. The prefetch trick in
  §2 and the "first probe pays no range fetch" UX both depend on cache-not-
  bypass semantics.
- **Reader lifecycle:** the worker opens with `await AsyncHttpFile(url).open()`
  *without* a context manager (the reader outlives the parse in the
  current-file slot) and later calls `await f.close()`; `close()` being async
  (unlike BytesIO's) is special-cased in `_set_current`.
