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
        if pending:
            await asyncio.gather(*pending)
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
