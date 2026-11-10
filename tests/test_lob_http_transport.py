"""HTTP display reads must share bounded gateway capacity without spurious 503s."""
import asyncio
import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

TOOLS = Path(__file__).resolve().parents[1] / 'lob_v2' / 'tools'
spec = importlib.util.spec_from_file_location('lob_http_transport_under_test', TOOLS / 'http_books.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_four_reference_books_do_not_reject_a_fifth_display_read(tmp_path):
    async def run():
        books = module.Books(tmp_path)
        release = asyncio.Event()
        running = peak = 0
        calls = []

        async def read(token, other):
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            calls.append(token)
            await release.wait()
            running -= 1
            return {'token': token}

        books.read = read
        tasks = [asyncio.create_task(books.cached_read(str(i), '')) for i in range(5)]
        await asyncio.sleep(.01)
        # Crypto reference reads four assets in parallel; the visible market
        # must wait within the same deadline instead of failing immediately.
        release.set()
        values = await asyncio.gather(*tasks)
        assert len(values) == len(calls) == 5
        assert peak == 4
        assert books.pending == {}

    asyncio.run(run())


def test_queued_duplicate_is_single_flight_and_survives_client_cancellation(tmp_path):
    async def run():
        books = module.Books(tmp_path)
        release = asyncio.Event()
        calls = []

        async def read(token, other):
            calls.append(token)
            await release.wait()
            return {'token': token}

        books.read = read
        active = [asyncio.create_task(books.cached_read(str(i), '')) for i in range(4)]
        await asyncio.sleep(.01)
        cancelled = asyncio.create_task(books.cached_read('display', ''))
        duplicate = asyncio.create_task(books.cached_read('display', ''))
        await asyncio.sleep(.01)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        release.set()
        assert await duplicate == {'token': 'display'}
        await asyncio.gather(*active)
        assert calls.count('display') == 1

    asyncio.run(run())


def test_pending_admission_is_bounded(tmp_path):
    async def run():
        books = module.Books(tmp_path)
        release = asyncio.Event()
        async def read(*_):
            await release.wait()
            return {}

        books.read = read
        tasks = [asyncio.create_task(books.cached_read(str(i), '')) for i in range(16)]
        await asyncio.sleep(.01)
        with pytest.raises(web.HTTPServiceUnavailable):
            await books.cached_read('overflow', '')
        release.set()
        await asyncio.gather(*tasks)

    asyncio.run(run())


def test_completed_snapshot_is_not_discarded_when_peer_resets_during_close(tmp_path, monkeypatch):
    async def run():
        books = module.Books(tmp_path)
        books.route = lambda *_: ('market', {'host': '127.0.0.1', 'port': 1})
        writer = AsyncMock()
        writer.close = lambda: None
        writer.wait_closed.side_effect = ConnectionResetError('peer closed after handoff')
        monkeypatch.setattr(module.asyncio, 'open_connection', AsyncMock(return_value=(object(), writer)))
        monkeypatch.setattr(module, 'send', AsyncMock())
        monkeypatch.setattr(module, 'receive_snapshot', AsyncMock(return_value=(
            {'items': [], 'stream': 'stream'}, 'session')))
        monkeypatch.setattr(module, 'confirm_snapshot', AsyncMock())
        result = await books.read('1', '')
        assert result['stream'] == 'stream'
        assert result['bookStatus'] == 'unavailable'

    asyncio.run(run())


def test_failed_handoff_is_never_returned_as_a_snapshot(tmp_path, monkeypatch):
    async def run():
        books = module.Books(tmp_path)
        books.route = lambda *_: ('market', {'host': '127.0.0.1', 'port': 1})
        writer = AsyncMock()
        writer.close = lambda: None
        monkeypatch.setattr(module.asyncio, 'open_connection', AsyncMock(return_value=(object(), writer)))
        monkeypatch.setattr(module, 'send', AsyncMock())
        monkeypatch.setattr(module, 'receive_snapshot', AsyncMock(return_value=(
            {'items': [], 'stream': 'stream'}, 'session')))
        monkeypatch.setattr(module, 'confirm_snapshot', AsyncMock(side_effect=ValueError('BOOTSTRAP_CANCELLED')))
        with pytest.raises(ValueError, match='BOOTSTRAP_CANCELLED'):
            await books.read('1', '')

    asyncio.run(run())


def test_queue_time_is_part_of_the_deadline_and_releases_all_slots(tmp_path, monkeypatch):
    monkeypatch.setattr(module, 'READ_TIMEOUT_SECONDS', .03)

    async def run():
        books = module.Books(tmp_path)

        async def read(*_):
            await asyncio.Event().wait()

        books.read = read
        results = await asyncio.gather(*(books.cached_read(str(i), '') for i in range(8)),
                                       return_exceptions=True)
        assert all(isinstance(value, TimeoutError) for value in results)
        assert books.active == 0
        assert books.pending == {}
        books.read = AsyncMock(return_value={'current': True})
        assert await books.cached_read('new-token', '') == {'current': True}

    asyncio.run(run())


def test_transient_failure_recovers_without_fifteen_second_negative_cache(tmp_path):
    async def run():
        books = module.Books(tmp_path)
        books.read = AsyncMock(side_effect=[ConnectionResetError(), {'current': True}])
        with pytest.raises(ConnectionResetError):
            await books.cached_read('1', '')
        with pytest.raises(ConnectionResetError):
            await books.cached_read('1', '')
        assert books.read.await_count == 1
        await asyncio.sleep(2.05)
        assert await books.cached_read('1', '') == {'current': True}
        assert books.read.await_count == 2

    asyncio.run(run())
