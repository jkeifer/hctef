from __future__ import annotations

import asyncio
import os

from collections.abc import Callable
from pathlib import Path

import pytest

from hctef.aio.async_block_cache import AsyncBlockCache
from hctef.block_cache import DEFAULT_BLOCK_BYTES, BlockCache

type FetchLog = list[tuple[int, int]]

CONTENT = bytes(i % 256 for i in range(1050))


def make_fetch(log: FetchLog) -> Callable[[int, int], bytes]:
    def fetch(start: int, end: int) -> bytes:
        log.append((start, end))
        return CONTENT[start:end]

    return fetch


def make_cache(
    tmp_path: Path,
    log: FetchLog,
    *,
    size: int = len(CONTENT),
    block_size: int = 100,
    **kwargs: object,
) -> BlockCache:
    return BlockCache(
        'https://example.com/file.bin',
        size,
        make_fetch(log),
        cache_dir=str(tmp_path),
        block_size=block_size,
        **kwargs,  # type: ignore[arg-type]
    )


def test_wrong_length_fetch_rejected_not_cached(tmp_path: Path) -> None:
    from hctef.exceptions import HctefNetworkError

    def short_fetch(start: int, end: int) -> bytes:
        return CONTENT[start : end - 1]  # one byte short

    cache = BlockCache(
        'https://example.com/file.bin',
        len(CONTENT),
        short_fetch,
        cache_dir=str(tmp_path),
        block_size=100,
    )
    with pytest.raises(HctefNetworkError, match='refusing to cache'):
        cache.read(0, 50)
    # the bad bytes must not have been persisted
    assert not list(tmp_path.rglob('*.blk'))


def test_block_math_and_final_short_block(tmp_path: Path) -> None:
    log: FetchLog = []
    cache = make_cache(tmp_path, log)
    # 1050 bytes / 100 => 11 blocks, final block is 50 bytes.
    assert cache.read(0, len(CONTENT)) == CONTENT
    # Whole file is one contiguous run.
    assert log == [(0, 1050)]
    # Final short block on disk is 50 bytes.
    assert cache._block_path(10).stat().st_size == 50
    assert cache._block_path(0).stat().st_size == 100


def test_read_slice_within_blocks(tmp_path: Path) -> None:
    log: FetchLog = []
    cache = make_cache(tmp_path, log)
    assert cache.read(150, 250) == CONTENT[150:250]
    # Blocks 1 and 2 needed, contiguous => single fetch of [100, 300).
    assert log == [(100, 300)]


def test_coalesce_missing_runs(tmp_path: Path) -> None:
    log: FetchLog = []
    cache = make_cache(tmp_path, log)
    # Prime two non-adjacent blocks.
    cache.read(0, 50)  # block 0 -> fetch (0, 100)
    cache.read(250, 260)  # block 2 -> fetch (200, 300)
    log.clear()

    assert cache.read(0, len(CONTENT)) == CONTENT
    # Missing blocks 1 and 3..10 coalesce into two runs.
    assert log == [(100, 200), (300, 1050)]


def test_cache_hit_no_refetch(tmp_path: Path) -> None:
    log: FetchLog = []
    cache = make_cache(tmp_path, log)
    cache.read(0, 300)
    log.clear()
    cache.read(50, 250)
    assert log == []


def test_persistence_across_reopen(tmp_path: Path) -> None:
    log1: FetchLog = []
    make_cache(tmp_path, log1).read(0, 300)
    assert log1 == [(0, 300)]

    log2: FetchLog = []
    cache2 = make_cache(tmp_path, log2)
    assert cache2.read(0, 300) == CONTENT[0:300]
    assert log2 == []  # served entirely from disk


def test_meta_mismatch_wipes(tmp_path: Path) -> None:
    log1: FetchLog = []
    make_cache(tmp_path, log1, etag='v1').read(0, 300)

    log2: FetchLog = []
    cache2 = make_cache(tmp_path, log2, etag='v2')
    cache2.read(0, 300)
    assert log2 == [(0, 300)]  # wiped -> refetched


def test_immutable_skips_validation(tmp_path: Path) -> None:
    log1: FetchLog = []
    make_cache(tmp_path, log1, etag='v1', immutable=True).read(0, 300)

    log2: FetchLog = []
    cache2 = make_cache(tmp_path, log2, etag='v2', immutable=True)
    cache2.read(0, 300)
    assert log2 == []  # not wiped despite differing etag


def test_block_size_mismatch_always_wipes(tmp_path: Path) -> None:
    log1: FetchLog = []
    make_cache(tmp_path, log1, block_size=100, immutable=True).read(0, 300)

    log2: FetchLog = []
    cache2 = make_cache(tmp_path, log2, block_size=200, immutable=True)
    cache2.read(0, 300)
    assert log2 == [(0, 400)]


def test_eviction_respects_max_bytes_and_lru(tmp_path: Path) -> None:
    log: FetchLog = []
    cache = make_cache(tmp_path, log, max_bytes=300)

    # Fill blocks 0, 1, 2 in separate reads (each its own in-flight run).
    cache.read(0, 100)
    cache.read(100, 200)
    cache.read(200, 300)

    # Force a deterministic LRU order: block 0 oldest.
    for index, mtime in ((0, 1000), (1, 2000), (2, 3000)):
        os.utime(cache._block_path(index), (mtime, mtime))

    # Writing block 3 pushes total to 400 > 300; evict LRU block 0.
    cache.read(300, 400)

    assert not cache._block_path(0).exists()
    assert cache._block_path(1).exists()
    assert cache._block_path(2).exists()
    assert cache._block_path(3).exists()


def test_atomic_write_uses_tmp_file(tmp_path: Path, monkeypatch) -> None:
    from hctef import block_cache

    srcs: list[str] = []
    real_replace = os.replace

    def spy_replace(src, dst):
        srcs.append(str(src))
        return real_replace(src, dst)

    monkeypatch.setattr(block_cache.os, 'replace', spy_replace)

    log: FetchLog = []
    cache = make_cache(tmp_path, log)
    cache.read(0, 300)

    assert srcs  # meta + blocks all went through os.replace
    assert all(src.endswith('.tmp') for src in srcs)
    # No torn tmp files left behind.
    assert list(cache.url_dir.glob('*.tmp')) == []


def test_env_var_config(tmp_path: Path, monkeypatch) -> None:
    cache_dir = tmp_path / 'envcache'
    monkeypatch.setenv('HCTEF_CACHE_DIR', str(cache_dir))
    monkeypatch.setenv('HCTEF_CACHE_BLOCK_BYTES', '256')
    monkeypatch.setenv('HCTEF_CACHE_MAX_BYTES', '4096')
    monkeypatch.setenv('HCTEF_CACHE_IMMUTABLE', 'yes')

    cache = BlockCache('https://example.com/x', 1000, make_fetch([]))
    assert cache.block_size == 256
    assert cache.max_bytes == 4096
    assert cache.immutable is True
    assert cache.cache_dir == cache_dir

    # Explicit args win over env vars.
    explicit = BlockCache(
        'https://example.com/x',
        1000,
        make_fetch([]),
        cache_dir=str(tmp_path),
        block_size=512,
        immutable=False,
    )
    assert explicit.block_size == 512
    assert explicit.immutable is False
    assert explicit.cache_dir == tmp_path


def test_default_tempdir_cleaned_on_close(monkeypatch) -> None:
    monkeypatch.delenv('HCTEF_CACHE_DIR', raising=False)
    monkeypatch.delenv('HCTEF_CACHE_BLOCK_BYTES', raising=False)
    cache = BlockCache('https://example.com/x', 1000, make_fetch([]))
    assert cache.block_size == DEFAULT_BLOCK_BYTES
    tmp = cache.cache_dir
    assert tmp.exists()
    cache.close()
    assert not tmp.exists()


@pytest.mark.asyncio
async def test_async_concurrent_reads_dedupe_fetch(tmp_path: Path) -> None:
    log: FetchLog = []

    async def fetch(start: int, end: int) -> bytes:
        log.append((start, end))
        await asyncio.sleep(0.01)
        return CONTENT[start:end]

    cache = AsyncBlockCache(
        'https://example.com/file.bin',
        len(CONTENT),
        fetch,
        cache_dir=str(tmp_path),
        block_size=100,
    )

    results = await asyncio.gather(
        cache.read(0, 100),
        cache.read(0, 100),
        cache.read(50, 150),
    )
    assert results[0] == CONTENT[0:100]
    assert results[2] == CONTENT[50:150]
    # Overlapping concurrent reads share in-flight fetches: blocks 0 and 1
    # are each fetched exactly once.
    assert sorted(log) == [(0, 100), (100, 200)]
