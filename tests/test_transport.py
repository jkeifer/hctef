import importlib
import sys
import types

from collections.abc import Callable
from typing import Any

import pytest

from hctef.aio import AsyncHttpFile
from hctef.aio.aiohttp_transport import AiohttpTransport
from hctef.aio.pyfetch_transport import PyfetchTransport
from hctef.aio.transport import (
    AsyncTransport,
    RemoteFileInfo,
    create_transport,
    default_transport_name,
)
from hctef.exceptions import HctefNetworkError

URL = 'http://example.com/file.bin'
DATA = bytes(range(256)) * 4  # 1 KiB of predictable data


class FakeTransport:
    """
    In-memory AsyncTransport for ownership tests (structural conformance).
    """

    def __init__(
        self,
        data: bytes = DATA,
        probe_error: Exception | None = None,
    ) -> None:
        self.data = data
        self.probe_error = probe_error
        self.closed = False
        self.fetches: list[tuple[int, int]] = []

    async def probe(self, url: str) -> RemoteFileInfo:
        if self.probe_error is not None:
            raise self.probe_error
        return RemoteFileInfo(len(self.data), '"fake-etag"', None)

    async def fetch_range(self, url: str, start: int, end: int) -> bytes:
        self.fetches.append((start, end))
        return self.data[start:end]

    async def close(self) -> None:
        self.closed = True


class VanillaTransport:
    """
    An AsyncTransport that imports/subclasses nothing from hctef.

    probe() deliberately has no return annotation: at runtime its result
    only needs .size/.etag/.last_modified attributes (duck typing), so a
    plain namespace object suffices.
    """

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.closed = False
        self.fetches: list[tuple[int, int]] = []

    async def probe(self, url: str):
        return types.SimpleNamespace(
            size=len(self.data),
            etag='"vanilla-etag"',
            last_modified=None,
        )

    async def fetch_range(self, url: str, start: int, end: int) -> bytes:
        self.fetches.append((start, end))
        return self.data[start:end]

    async def close(self) -> None:
        self.closed = True


def _transport_conformance(
    aiohttp_transport: AiohttpTransport,
    pyfetch_transport: PyfetchTransport,
    fake_transport: FakeTransport,
) -> tuple[AsyncTransport, AsyncTransport, AsyncTransport]:
    """Never called: mypy-level proof these satisfy the AsyncTransport protocol."""
    return aiohttp_transport, pyfetch_transport, fake_transport


class FakeResponse:
    """Minimal stand-in for pyodide.http.FetchResponse."""

    def __init__(
        self,
        status: int,
        headers: dict[str, str],
        body: bytes = b'',
    ) -> None:
        self.status = status
        self.headers = headers
        self._body = body

    async def bytes(self) -> bytes:
        return self._body


def _install_fake_pyodide(
    monkeypatch: pytest.MonkeyPatch,
    pyfetch: Callable[..., Any],
) -> None:
    """Inject a fake pyodide.http module exposing the given pyfetch."""
    http_mod = types.ModuleType('pyodide.http')
    http_mod.pyfetch = pyfetch  # type: ignore[attr-defined]
    pyodide_mod = types.ModuleType('pyodide')
    pyodide_mod.http = http_mod  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, 'pyodide', pyodide_mod)
    monkeypatch.setitem(sys.modules, 'pyodide.http', http_mod)


@pytest.fixture
def fake_pyfetch(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """
    Install a fake pyfetch serving DATA with proper range support.

    Returns the call log; each entry has 'url' and 'headers'.
    """
    calls: list[dict[str, Any]] = []

    async def pyfetch(url: str, **kwargs: Any) -> FakeResponse:
        headers = kwargs.get('headers') or {}
        calls.append({'url': url, 'headers': headers})
        start_s, _, end_s = (
            headers['Range']
            .removeprefix('bytes=')
            .partition(
                '-',
            )
        )
        start = int(start_s)
        end = min(int(end_s) if end_s else len(DATA) - 1, len(DATA) - 1)
        # Mixed-case header names to prove the transport normalizes them
        resp_headers = {
            'Content-Range': f'bytes {start}-{end}/{len(DATA)}',
            'ETag': '"fake-etag"',
            'Last-Modified': 'Mon, 01 Jan 2024 00:00:00 GMT',
        }
        return FakeResponse(206, resp_headers, DATA[start : end + 1])

    _install_fake_pyodide(monkeypatch, pyfetch)
    return calls


def _purge_hctef_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(sys.modules):
        if name == 'hctef' or name.startswith('hctef.'):
            monkeypatch.delitem(sys.modules, name)


def _hide_module(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """Make `import <name>` raise ImportError."""
    monkeypatch.setitem(sys.modules, name, None)  # type: ignore[arg-type]


# -- import behavior ---------------------------------------------------------


def test_aio_imports_without_aiohttp_or_pyodide(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_hctef_modules(monkeypatch)
    _hide_module(monkeypatch, 'aiohttp')
    _hide_module(monkeypatch, 'pyodide')
    aio = importlib.import_module('hctef.aio')
    assert hasattr(aio, 'AsyncHttpFile')
    # Construction must also work without either backing library
    hf = aio.AsyncHttpFile(URL)
    assert hf.url == URL


def test_missing_aiohttp_errors_at_transport_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delitem(
        sys.modules,
        'hctef.aio.aiohttp_transport',
        raising=False,
    )
    _hide_module(monkeypatch, 'aiohttp')
    with pytest.raises(ImportError, match=r'\[async\]'):
        create_transport('aiohttp')


@pytest.mark.asyncio
async def test_missing_aiohttp_errors_at_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delitem(
        sys.modules,
        'hctef.aio.aiohttp_transport',
        raising=False,
    )
    _hide_module(monkeypatch, 'aiohttp')
    hf = AsyncHttpFile(URL)
    with pytest.raises(ImportError, match=r'\[async\]'):
        await hf.open()


def test_missing_pyodide_errors_at_transport_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _hide_module(monkeypatch, 'pyodide')
    _hide_module(monkeypatch, 'pyodide.http')
    with pytest.raises(ImportError, match='Pyodide'):
        create_transport('pyfetch')


# -- transport selection -----------------------------------------------------


def test_default_transport_name_emscripten(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, 'platform', 'emscripten')
    assert default_transport_name() == 'pyfetch'


def test_default_transport_name_cpython() -> None:
    assert sys.platform != 'emscripten'
    assert default_transport_name() == 'aiohttp'


def test_create_transport_default_emscripten_is_pyfetch(
    monkeypatch: pytest.MonkeyPatch,
    fake_pyfetch: list[dict[str, Any]],
) -> None:
    monkeypatch.setattr(sys, 'platform', 'emscripten')
    transport = create_transport()
    assert isinstance(transport, PyfetchTransport)


@pytest.mark.asyncio
async def test_create_transport_default_cpython_is_aiohttp() -> None:
    transport = create_transport()
    assert isinstance(transport, AiohttpTransport)
    await transport.close()


@pytest.mark.asyncio
async def test_explicit_override_wins_over_platform_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, 'platform', 'emscripten')
    transport = create_transport('aiohttp')
    assert isinstance(transport, AiohttpTransport)
    await transport.close()


def test_explicit_pyfetch_on_cpython(
    fake_pyfetch: list[dict[str, Any]],
) -> None:
    transport = create_transport('pyfetch')
    assert isinstance(transport, PyfetchTransport)


def test_pyfetch_rejects_session_kwargs(
    fake_pyfetch: list[dict[str, Any]],
) -> None:
    with pytest.raises(ValueError, match='session_kwargs'):
        create_transport('pyfetch', {'headers': {'X-Test': '1'}})


def test_unknown_transport_name() -> None:
    with pytest.raises(ValueError, match='Unknown transport'):
        create_transport('carrier-pigeon')  # type: ignore[arg-type]


# -- pyfetch transport behavior ----------------------------------------------


@pytest.mark.asyncio
async def test_pyfetch_probe(fake_pyfetch: list[dict[str, Any]]) -> None:
    transport = create_transport('pyfetch')
    info = await transport.probe(URL)
    assert info.size == len(DATA)
    assert info.etag == '"fake-etag"'
    assert info.last_modified == 'Mon, 01 Jan 2024 00:00:00 GMT'
    # Probe uses a one-byte range so browsers don't stream the whole body
    assert fake_pyfetch[0]['headers']['Range'] == 'bytes=0-0'
    await transport.close()


@pytest.mark.asyncio
async def test_pyfetch_fetch_range(
    fake_pyfetch: list[dict[str, Any]],
) -> None:
    transport = create_transport('pyfetch')
    data = await transport.fetch_range(URL, 10, 20)
    assert data == DATA[10:20]
    # Range header end is inclusive per HTTP semantics
    assert fake_pyfetch[-1]['headers']['Range'] == 'bytes=10-19'
    await transport.close()


@pytest.mark.asyncio
async def test_pyfetch_fetch_range_non_206(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def pyfetch(url: str, **kwargs: Any) -> FakeResponse:
        return FakeResponse(200, {}, DATA)

    _install_fake_pyodide(monkeypatch, pyfetch)
    transport = create_transport('pyfetch')
    with pytest.raises(HctefNetworkError, match='206'):
        await transport.fetch_range(URL, 0, 10)


@pytest.mark.asyncio
async def test_pyfetch_probe_cors_hidden_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A CORS response without Access-Control-Expose-Headers shows no
    # Content-Range even when the server honored the Range request
    async def pyfetch(url: str, **kwargs: Any) -> FakeResponse:
        return FakeResponse(206, {'content-type': 'application/octet-stream'})

    _install_fake_pyodide(monkeypatch, pyfetch)
    transport = create_transport('pyfetch')
    with pytest.raises(
        HctefNetworkError,
        match='Access-Control-Expose-Headers',
    ):
        await transport.probe(URL)


@pytest.mark.asyncio
async def test_pyfetch_probe_network_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def pyfetch(url: str, **kwargs: Any) -> FakeResponse:
        raise OSError('network unreachable')

    _install_fake_pyodide(monkeypatch, pyfetch)
    transport = create_transport('pyfetch')
    with pytest.raises(
        HctefNetworkError,
        match='Cannot determine file size',
    ):
        await transport.probe(URL)


@pytest.mark.asyncio
async def test_pyfetch_fetch_after_close_raises(
    fake_pyfetch: list[dict[str, Any]],
) -> None:
    transport = create_transport('pyfetch')
    await transport.close()
    with pytest.raises(RuntimeError, match='Session is closed'):
        await transport.fetch_range(URL, 0, 1)


# -- AsyncHttpFile end-to-end over pyfetch -----------------------------------


@pytest.mark.asyncio
async def test_async_http_file_pyfetch_end_to_end(
    fake_pyfetch: list[dict[str, Any]],
    tmp_path: Any,
) -> None:
    async with AsyncHttpFile(
        URL,
        transport='pyfetch',
        block_size=64,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
    ) as hf:
        assert hf.size == len(DATA)

        assert await hf.read(10) == DATA[:10]
        assert hf.tell() == 10

        hf.seek(-16, 2)
        assert await hf.read() == DATA[-16:]

        # Cached data must not trigger more fetches
        n_calls = len(fake_pyfetch)
        hf.seek(0)
        assert await hf.read(10) == DATA[:10]
        assert len(fake_pyfetch) == n_calls

    assert all(c['url'] == URL for c in fake_pyfetch)


@pytest.mark.asyncio
async def test_async_http_file_pyfetch_default_on_emscripten(
    monkeypatch: pytest.MonkeyPatch,
    fake_pyfetch: list[dict[str, Any]],
    tmp_path: Any,
) -> None:
    monkeypatch.setattr(sys, 'platform', 'emscripten')
    async with AsyncHttpFile(
        URL,
        block_size=128,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
    ) as hf:
        assert isinstance(hf.cursor.ohf.transport, PyfetchTransport)
        assert await hf.read(5) == DATA[:5]
    assert len(fake_pyfetch) > 0


@pytest.mark.asyncio
async def test_async_http_file_pyfetch_rejects_session_kwargs(
    fake_pyfetch: list[dict[str, Any]],
) -> None:
    hf = AsyncHttpFile(
        URL,
        transport='pyfetch',
        session_kwargs={'headers': {'X-Test': '1'}},
    )
    with pytest.raises(ValueError, match='session_kwargs'):
        await hf.open()


# -- injected transport instances ----------------------------------------------


@pytest.mark.asyncio
async def test_injected_hctef_free_transport_end_to_end(tmp_path: Any) -> None:
    transport = VanillaTransport(DATA)
    async with AsyncHttpFile(
        URL,
        transport=transport,
        block_size=64,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
    ) as hf:
        assert hf.size == len(DATA)

        assert await hf.read(10) == DATA[:10]
        assert hf.tell() == 10

        hf.seek(-16, 2)
        assert await hf.read() == DATA[-16:]

        # Cached data must not trigger more fetches
        n_fetches = len(transport.fetches)
        hf.seek(0)
        assert await hf.read(10) == DATA[:10]
        assert len(transport.fetches) == n_fetches

    # The injected transport actually served the reads...
    assert transport.fetches
    # ...but is caller-owned: AsyncHttpFile.close() must not close it
    assert not transport.closed
    await transport.close()


@pytest.mark.asyncio
async def test_injected_transport_not_closed_by_close(tmp_path: Any) -> None:
    transport = FakeTransport()
    hf = AsyncHttpFile(
        URL,
        transport=transport,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
    )
    await hf.open()
    await hf.close()
    assert not transport.closed


@pytest.mark.asyncio
async def test_injected_transport_not_closed_on_probe_failure() -> None:
    transport = FakeTransport(probe_error=HctefNetworkError('probe failed'))
    hf = AsyncHttpFile(URL, transport=transport)
    with pytest.raises(HctefNetworkError, match='probe failed'):
        await hf.open()
    assert not transport.closed


@pytest.mark.asyncio
async def test_name_created_transport_closed_by_close(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    transport = FakeTransport()
    monkeypatch.setattr(
        'hctef.aio.async_http_file.create_transport',
        lambda *args, **kwargs: transport,
    )
    hf = AsyncHttpFile(URL, prefetch_bytes=0, cache_dir=str(tmp_path))
    await hf.open()
    await hf.close()
    assert transport.closed


@pytest.mark.asyncio
async def test_name_created_transport_closed_on_probe_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = FakeTransport(probe_error=HctefNetworkError('probe failed'))
    monkeypatch.setattr(
        'hctef.aio.async_http_file.create_transport',
        lambda *args, **kwargs: transport,
    )
    hf = AsyncHttpFile(URL)
    with pytest.raises(HctefNetworkError, match='probe failed'):
        await hf.open()
    assert transport.closed


@pytest.mark.asyncio
async def test_injected_transport_rejects_session_kwargs() -> None:
    transport = FakeTransport()
    hf = AsyncHttpFile(
        URL,
        transport=transport,
        session_kwargs={'headers': {'X-Test': '1'}},
    )
    with pytest.raises(ValueError, match='session_kwargs'):
        await hf.open()
    # Rejection happens before probe; the instance stays caller-owned/open
    assert not transport.closed
