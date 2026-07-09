import os
import urllib.error
import urllib.request

from pathlib import Path
from typing import Any

import pytest

from hctef import HttpFile
from hctef.exceptions import HctefNetworkError, RangeRequestsUnsupportedError
from hctef.http_file import DEFAULT_TIMEOUT_SECONDS

URL = 'http://example.com/file.bin'


class FakeUrlopenResponse:
    def __init__(
        self,
        status: int,
        headers: dict[str, str],
        body: bytes = b'',
    ) -> None:
        self.status = status
        self.headers = headers
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> 'FakeUrlopenResponse':
        return self

    def __exit__(self, *exc: object) -> None:
        return None


UrlopenOutcome = Exception | FakeUrlopenResponse
UrlopenCall = tuple[urllib.request.Request, float | None]


@pytest.fixture
def scripted_urlopen(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[UrlopenOutcome], list[UrlopenCall]]:
    """Replace urlopen with a scripted fake; returns (script, call log)."""
    script: list[UrlopenOutcome] = []
    calls: list[UrlopenCall] = []

    def urlopen(request: urllib.request.Request, timeout: float | None = None) -> Any:
        calls.append((request, timeout))
        outcome = script.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(urllib.request, 'urlopen', urlopen)
    return script, calls


def _probe_ok() -> FakeUrlopenResponse:
    return FakeUrlopenResponse(206, {'Content-Range': 'bytes 0-0/1050'}, b'\x00')


def test_probe_one_byte_range_and_timeout(
    scripted_urlopen: tuple[list[UrlopenOutcome], list[UrlopenCall]],
    tmp_path: Path,
) -> None:
    script, calls = scripted_urlopen
    script.append(_probe_ok())
    with HttpFile(URL, prefetch_bytes=0, cache_dir=str(tmp_path)) as hf:
        assert hf._ohf._size == 1050
    request, timeout = calls[0]
    # One-byte range so the server doesn't stream the whole body
    assert request.get_header('Range') == 'bytes=0-0'
    assert timeout == DEFAULT_TIMEOUT_SECONDS


def test_probe_http_error_names_status(
    scripted_urlopen: tuple[list[UrlopenOutcome], list[UrlopenCall]],
    tmp_path: Path,
) -> None:
    script, _ = scripted_urlopen
    script.append(
        urllib.error.HTTPError(URL, 404, 'Not Found', None, None),  # type: ignore[arg-type]
    )
    with pytest.raises(HctefNetworkError, match='404'):
        HttpFile(URL, prefetch_bytes=0, cache_dir=str(tmp_path)).open()


def test_fetch_range_rejects_non_206(
    scripted_urlopen: tuple[list[UrlopenOutcome], list[UrlopenCall]],
    tmp_path: Path,
) -> None:
    script, _ = scripted_urlopen
    # Probe succeeds; the data fetch gets a 200 whose full body must not be
    # treated as the requested slice.
    script.extend([_probe_ok(), FakeUrlopenResponse(200, {}, bytes(1050))])
    with HttpFile(URL, prefetch_bytes=0, cache_dir=str(tmp_path)) as hf:
        with pytest.raises(RangeRequestsUnsupportedError) as excinfo:
            hf.read(10)
        assert excinfo.value.reason == 'no-range-support'


@pytest.mark.parametrize('parquet_file_name', ['alltypes_plain'])
def test_http_file(parquet_url: str) -> None:
    with HttpFile(
        parquet_url,
        block_size=80,
        prefetch_bytes=100,
    ) as hf:
        # Test initial state
        assert hf.tell() == 0

        # Test reading a chunk
        data = hf.read(100)
        assert len(data) == 100
        assert hf.tell() == 100

        # Test reading another chunk
        data2 = hf.read(50)
        assert len(data2) == 50
        assert hf.tell() == 150

        # Test seeking from the beginning of the file (SEEK_SET)
        hf.seek(0, os.SEEK_SET)
        assert hf.tell() == 0
        data3 = hf.read(10)
        assert len(data3) == 10
        assert hf.tell() == 10

        # Test seeking from the current position (SEEK_CUR)
        hf.seek(20, os.SEEK_CUR)
        assert hf.tell() == 30
        data4 = hf.read(20)
        assert len(data4) == 20
        assert hf.tell() == 50

        # Test seeking from the end of the file (SEEK_END)
        hf.seek(-50, os.SEEK_END)
        file_size = hf._ohf._size
        assert hf.tell() == file_size - 50
        data5 = hf.read(50)
        assert len(data5) == 50
        assert hf.tell() == file_size

        # Test reading past the end of the file
        assert hf.read() == b''
        assert hf.tell() == file_size

        # Test seeking past the end of the file
        hf.seek(1000, os.SEEK_END)
        assert hf.tell() == file_size  # Position should be clamped
        assert hf.read() == b''

        # Test seeking before the beginning of the file
        hf.seek(-file_size - 100, os.SEEK_END)
        assert hf.tell() == 0  # Position should be clamped


def test_prefetch_passthrough(
    scripted_urlopen: tuple[list[UrlopenOutcome], list[UrlopenCall]],
    tmp_path: Path,
) -> None:
    script, calls = scripted_urlopen
    body = bytes(i % 256 for i in range(1050))
    script.append(_probe_ok())
    script.append(
        FakeUrlopenResponse(
            206,
            {'Content-Range': 'bytes 0-199/1050'},
            body[:200],
        ),
    )
    with HttpFile(
        URL,
        prefetch_bytes=0,
        cache_dir=str(tmp_path),
        block_size=100,
    ) as hf:
        assert hf.prefetch([(0, 100), (100, 100)]) == 200
        # Warmed reads are served from cache: no further urlopen calls
        n_calls = len(calls)
        assert hf.read(200) == body[:200]
        assert len(calls) == n_calls


def test_prefetch_on_closed_file_raises() -> None:
    hf = HttpFile(URL)
    with pytest.raises(ValueError, match='closed file'):
        hf.prefetch([(0, 10)])
