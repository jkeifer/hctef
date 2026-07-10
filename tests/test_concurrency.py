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
