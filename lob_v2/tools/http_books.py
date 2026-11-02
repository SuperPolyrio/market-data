#!/usr/bin/env python3
"""Bounded read-only HTTP bridge to committed LOB V2 snapshots.

No exchange connections, journal replay, database writes or collector startup.
"""
import argparse
import asyncio
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import re
import sqlite3
import sys
import time

PACKAGE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PACKAGE / ('.vendor312' if sys.version_info >= (3, 12) else '.vendor')), str(PACKAGE)]
from aiohttp import web
from lobv2.node import receive_snapshot, confirm_snapshot
from lobv2.wire import control, send


def iso(value):
    return value.isoformat().replace('+00:00', 'Z')


def side_payload(token, asset, observed, quality):
    asset = asset or {}
    source_at = None
    try:
        source_at = datetime.fromtimestamp(int(asset['last_source_time']) / 1000, timezone.utc)
    except (KeyError, ValueError, TypeError, OverflowError):
        pass
    # Reading a snapshot proves gateway reachability, not upstream freshness.
    # Older observation-only collectors cannot satisfy the live proof contract.
    live = bool(asset.get('ready') and quality.get('transport_healthy') is True
                and quality.get('transport_policy') == 'fresh_pong_rtt_v1'
                and source_at and 0 <= (observed - source_at).total_seconds() <= 15)
    bids = [{'price': float(p), 'size': float(s)} for p, s in asset.get('bids', [])[:12]]
    asks = [{'price': float(p), 'size': float(s)} for p, s in asset.get('asks', [])[:12]]
    bid, ask = (bids[0]['price'] if bids else None), (asks[0]['price'] if asks else None)
    return {'tokenId': token, 'bids': bids, 'asks': asks, 'bestBid': bid, 'bestAsk': ask,
            'spread': ask - bid if bid is not None and ask is not None else None,
            'bookStatus': 'live' if live else 'stale' if asset else 'unavailable',
            'continuity': live, 'receivedAt': iso(source_at) if source_at else None,
            'timestampOrigin': 'upstream-event-time', 'observedAt': iso(observed),
            'heartbeatAt': iso(observed) if live else None,
            'staleAfter': iso(source_at + timedelta(seconds=20)) if live else None,
            'upstreamReady': bool(asset.get('ready')), 'quality': quality,
            'sourceRef': asset.get('last_ref')}


class Books:
    def __init__(self, root):
        self.root = Path(root)
        self.cache = OrderedDict()
        self.pending = {}
        self.maximum = 4
        self.last_read = None

    def route(self, token, other):
        # Catalog identity and ownership are authoritative; clients cannot choose a port.
        with sqlite3.connect(f'file:{self.root / "catalog.sqlite"}?mode=ro', uri=True, timeout=0.05) as db:
            row = db.execute('SELECT market FROM token_owners WHERE token=?', (token,)).fetchone()
            if not row:
                raise web.HTTPNotFound(text='Token is not in the LOB V2 catalog')
            market = row[0]
            if other and db.execute('SELECT market FROM token_owners WHERE token=?', (other,)).fetchone() != (market,):
                raise web.HTTPBadRequest(text='Token pair belongs to different markets')
            row = db.execute('SELECT cohort FROM catalog_assignments WHERE market=?', (market,)).fetchone()
        if not row or not re.fullmatch(r'[0-9]+', row[0]):
            raise web.HTTPServiceUnavailable(text='Market has no gateway assignment')
        config = json.loads((self.root / 'instances' / f'G-{row[0]}.json').read_bytes())['config']['listen']
        if config['host'] not in ('127.0.0.1', '::1', 'localhost'):
            raise ValueError('Only local committed gateways may be read')
        return market, config

    async def read(self, token, other):
        market, config = self.route(token, other)
        reader, writer = await asyncio.open_connection(config['host'], config['port'])
        try:
            await send(writer, control('HELLO', {'schema': 2, 'markets': [market]}))
            snapshot, session = await receive_snapshot(reader)
            await confirm_snapshot(reader, writer, snapshot, session)
        finally:
            writer.close()
            await writer.wait_closed()
        items = snapshot['items']
        if len(items) > 1 or (items and items[0]['market'] != market):
            raise ValueError('Gateway snapshot identity mismatch')
        observed = datetime.now(timezone.utc)
        item = items[0] if items else {}
        assets, quality = item.get('assets', {}), item.get('quality', {})
        yes, no = side_payload(token, assets.get(token), observed, quality), side_payload(other, assets.get(other), observed, quality)
        states = {yes['bookStatus'], no['bookStatus']} if other else {yes['bookStatus']}
        self.last_read = time.monotonic()
        return {'source': 'market-data', 'runtimeModel': 'lob-v2', 'tokenMode': True,
                'conditionId': market, 'generatedAt': iso(observed),
                'yes': yes, 'no': no, 'bookStatus': next(s for s in ('unavailable', 'stale', 'live') if s in states),
                'cut': item.get('cut'), 'stream': snapshot['stream'],
                'upstreamCompleteness': quality.get('upstream_completeness', 'UNKNOWN')}

    async def cached_read(self, token, other):
        key = token, other
        cached = self.cache.get(key)
        if cached and cached[0] > time.monotonic():
            self.cache.move_to_end(key)
            if isinstance(cached[1], Exception):
                raise cached[1]
            return cached[1]
        if key in self.pending:
            return await asyncio.shield(self.pending[key])
        if len(self.pending) >= self.maximum:
            raise web.HTTPServiceUnavailable(text='Book reader is at capacity', headers={'Retry-After': '5'})

        async def build():
            try:
                payload = await asyncio.wait_for(self.read(token, other), 2.0)
                self.cache[key] = time.monotonic() + 2, payload
                return payload
            except Exception as error:
                self.cache[key] = time.monotonic() + 15, error
                raise
            finally:
                self.pending.pop(key, None)
                while len(self.cache) > 128:
                    self.cache.popitem(last=False)

        task = asyncio.create_task(build())
        self.pending[key] = task
        # Retrieve failures even when the HTTP client disconnects.
        task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        return await asyncio.shield(task)


def create_app(root):
    books = Books(root)
    app = web.Application(client_max_size=1024)

    async def book(request):
        token, other = request.match_info['token'], request.query.get('noTokenId', '')
        if not re.fullmatch(r'[0-9]{1,100}', token) or other and not re.fullmatch(r'[0-9]{1,100}', other):
            raise web.HTTPBadRequest(text='Invalid token identity')
        try:
            payload = await books.cached_read(token, other)
            return web.json_response(payload, headers={'Cache-Control': 'no-store'})
        except web.HTTPException:
            raise
        except Exception:
            return web.json_response({'status': 'unavailable', 'source': 'market-data', 'runtimeModel': 'lob-v2'},
                                     status=503, headers={'Retry-After': '15', 'Cache-Control': 'no-store'})

    async def health(request):
        ready = books.last_read is not None and time.monotonic() - books.last_read < 60
        return web.json_response({'source': 'market-data', 'runtimeModel': 'lob-v2',
                                  'status': 'ok' if ready else 'unverified', 'activeReads': len(books.pending),
                                  'maxConcurrentReads': books.maximum, 'cachedBooks': len(books.cache),
                                  'upstreamCompleteness': 'UNKNOWN'}, status=200 if ready else 503)

    app.router.add_get('/book/{token}', book)
    app.router.add_get('/health', health)
    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--port', type=int, default=18861)
    args = parser.parse_args()
    web.run_app(create_app(args.root), host='127.0.0.1', port=args.port, access_log=None)
