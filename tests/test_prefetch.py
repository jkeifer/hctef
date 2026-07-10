"""
Regression tests for open()-time END-direction prefetch.

AsyncHttpFile.open() used to seek to +prefetch_bytes from the end of the
file (clamped to EOF), so the END prefetch read zero bytes and never
populated the cache. These tests assert the fake transport actually saw
range requests covering the file tail during open().
"""

import asyncio
import gc

from typing import Any

import pytest

from hctef.aio import AsyncHttpFile
from hctef.exceptions import HctefNetworkError

from .test_transport import (  # noqa: F401
    DATA,
    URL,
    FakeTransport,
    fake_pyfetch,
)


def _covered_bytes(calls: list[dict[str, Any]]) -> set[int]:
    """Union of byte offsets covered by all Range requests seen."""
    covered: set[int] = set()
    for call in calls:
        start_s, _, end_s = (
            call['headers']['Range'].removeprefix('bytes=').partition('-')
        )
        start = int(start_s)
        end = min(int(end_s) if end_s else len(DATA) - 1, len(DATA) - 1)
        covered.update(range(start, end + 1))
    return covered


@pytest.mark.asyncio
async def test_open_end_prefetch_fetches_tail(
    fake_pyfetch: list[dict[str, Any]],  # noqa: F811
    tmp_path: Any,
) -> None:
    prefetch = 100
    async with AsyncHttpFile(
        URL,
        transport='pyfetch',
        block_size=64,
        prefetch_bytes=prefetch,
        prefetch_direction='END',
        cache_dir=str(tmp_path),
    ) as hf:
        size = hf.size
        assert size == len(DATA)
        # The cursor must be rewound after prefetch
        assert hf.tell() == 0

        # The last `prefetch` bytes must have been requested during open()
        tail = set(range(size - prefetch, size))
        assert tail <= _covered_bytes(fake_pyfetch)

        # Reading the tail must be served from cache: no new requests
        n_calls = len(fake_pyfetch)
        hf.seek(-prefetch, 2)
        assert await hf.read() == DATA[-prefetch:]
        assert len(fake_pyfetch) == n_calls


@pytest.mark.asyncio
async def test_open_end_prefetch_larger_than_file(
    fake_pyfetch: list[dict[str, Any]],  # noqa: F811
    tmp_path: Any,
) -> None:
    # Prefetch larger than the file must clamp to the whole file
    async with AsyncHttpFile(
        URL,
        transport='pyfetch',
        block_size=64,
        prefetch_bytes=len(DATA) * 4,
        prefetch_direction='END',
        cache_dir=str(tmp_path),
    ) as hf:
        assert hf.tell() == 0

        # The entire file must have been requested during open()
        assert set(range(len(DATA))) <= _covered_bytes(fake_pyfetch)

        # Reading everything back must be served from cache
        n_calls = len(fake_pyfetch)
        assert await hf.read() == DATA
        assert len(fake_pyfetch) == n_calls


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


@pytest.mark.asyncio
async def test_prefetch_multi_run_failure_propagates_cleanly(
    tmp_path: Any,
) -> None:
    # Multiple disjoint failing runs: the error must propagate, and every
    # fan-out task's exception must be retrieved (no dangling-task noise).
    class FailingTransport(FakeTransport):
        async def fetch_range(self, url: str, start: int, end: int) -> bytes:
            await super().fetch_range(url, start, end)
            raise HctefNetworkError(f'boom {start}')

    unretrieved: list[str] = []

    def handler(loop: Any, context: dict[str, Any]) -> None:
        unretrieved.append(context.get('message', ''))

    transport = FailingTransport()
    async with AsyncHttpFile(
        URL,
        transport=transport,
        block_size=64,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
    ) as hf:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(handler)
        try:
            with pytest.raises(HctefNetworkError):
                # Three disjoint runs -> three fan-out tasks, all failing
                await hf.prefetch([(0, 10), (256, 10), (512, 10)])
            # All three runs were attempted (results retrieved via gather)
            assert len(transport.fetches) == 3
            await asyncio.sleep(0)
            gc.collect()
            await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(None)
    assert unretrieved == []
