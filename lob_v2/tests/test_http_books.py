import asyncio
from datetime import datetime, timezone
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools'))
from http_books import Books, side_payload
from aiohttp import web


def test_observation_only_snapshot_is_not_labeled_live():
    now = datetime.now(timezone.utc)
    asset = {'ready': True, 'bids': [['0.4', '10']], 'asks': [['0.5', '20']],
             'last_source_time': str(int(now.timestamp()*1000))}
    side = side_payload('123', asset, now, {'transport_healthy': True, 'transport_policy': 'OBSERVATION_ONLY'})
    assert side['bookStatus'] == 'stale'
    assert side['bids'][0]['price'] == .4
    assert side['continuity'] is False


def test_reader_singleflight_cache_and_capacity():
    async def run():
        books, gate, calls = Books('/unused'), asyncio.Event(), []
        async def read(token, other):
            calls.append(token)
            await gate.wait()
            return {'token': token}
        books.read = read
        tasks = [asyncio.create_task(books.cached_read('1', '2')) for _ in range(20)]
        for _ in range(4):
            await asyncio.sleep(0)
        assert calls == ['1']
        for token in ['3', '4', '5']:
            tasks.append(asyncio.create_task(books.cached_read(token, '')))
        await asyncio.sleep(0)
        try:
            await books.cached_read('6', '')
            assert False, 'capacity must reject extra work'
        except web.HTTPServiceUnavailable:
            pass
        gate.set()
        await asyncio.gather(*tasks)
        await books.cached_read('1', '2')
        assert len(calls) == 4
        assert not books.pending
    asyncio.run(run())


def test_reader_failure_cooldown():
    async def run():
        books, calls = Books('/unused'), []
        async def fail(*args):
            calls.append(1)
            raise ConnectionError('gateway unavailable')
        books.read = fail
        for _ in range(30):
            try:
                await books.cached_read('1', '')
                assert False
            except ConnectionError:
                pass
        assert len(calls) == 1
    asyncio.run(run())
