import asyncio
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'lob_v2/tools'))
import crypto_reference as reader
spec = importlib.util.spec_from_file_location('official_reference_archive', ROOT / 'src/prices/reference_reader.py')
archive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(archive)
START = 1793673000000


def point(at=START):
    return {'value': '100', 'full_accuracy_value': '100000000000000000000',
            'event_at_ms': at, 'feed_id': 'rtds:crypto_prices_twap_sixty:btc/usd'}


def market(asset='BTC', start=START):
    url = f'https://data.chain.link/streams/{asset.lower()}-usd-twap-60s-streams'
    raw = {'id': '123', 'slug': f'{asset.lower()}-updown-5m-{start//1000}',
           'conditionId': '0x'+'a'*64, 'version': 'v1', 'outcomes': '["Up","Down"]',
           'clobTokenIds': '["123","456"]', 'resolutionSource': url,
           'endDate': datetime.fromtimestamp((start+300000)/1000, timezone.utc).isoformat(),
           'description': 'time-weighted average price greater than or equal to the beginning. '+url}
    return raw


def test_exact_archive_opening_clocks_precision_and_rejections(tmp_path):
    day = datetime.fromtimestamp(START/1000, timezone.utc).date().isoformat()
    path = tmp_path/day/'twap.sqlite3'
    path.parent.mkdir()
    with sqlite3.connect(path) as db:
        db.execute('''CREATE TABLE records(frame_id,asset,window_seconds,error,duplicate,conflict,
            timestamp_order,price,full_accuracy_value,observed_ts,published_ts,received_ts,feed_id,source,topic)''')
        def insert(id, at, **patch):
            r = dict(frame_id=id,asset='btc/usd',window_seconds=60,error=None,duplicate=0,conflict=0,
                     timestamp_order=0,price='100',full_accuracy_value='100000000000000000000',
                     observed_ts=at,published_ts=at+100,received_ts=(at+200)*1000000,
                     feed_id='rtds:crypto_prices_twap_sixty:btc/usd',source='polymarket_rtds_chainlink',topic='crypto_prices_twap_sixty')
            r.update(patch)
            db.execute('INSERT INTO records VALUES ('+','.join('?' for _ in r)+')',tuple(r.values()))
        insert('before',START-1000)
        insert('anchor',START)
        insert('latest',START+1000)
        insert('duplicate',START+1000,duplicate=1)
        insert('conflict',START+2000,conflict=1)
        insert('bad-e18',START+3000,price='101')
        insert('bad-clock',START+4000,received_ts=(START+3000)*1000000)
        insert('other-feed',START+5000,feed_id='other')
    original = path.read_bytes()
    data=archive.read_archive(tmp_path,START+6000)['assets']['BTC']
    assert [p['frame_id'] for p in data['prices']]==['before','anchor','latest']
    assert data['opening']['event_at_ms']==START
    assert data['opening']['full_accuracy_value']=='100000000000000000000'
    assert path.read_bytes()==original
    assert archive.read_archive(tmp_path,START+300000)['assets']['BTC']['opening'] is None
    stale=archive.read_archive(tmp_path,START+1800000)['assets']['BTC']
    assert stale['opening'] is None
    assert stale['prices'][-1]['event_at_ms']==START+1000
    midnight=(START//86400000+1)*86400000
    assert archive.read_archive(tmp_path,midnight+60000)['assets']['BTC']['prices'][-1]['event_at_ms']==START+1000
    assert archive.read_archive(tmp_path/'missing',START)['assets']['BTC']['prices']==[]
    assert not (tmp_path/'missing').exists()


def test_binding_requires_exact_rules_interval_and_token_identity():
    raw=market()
    observed=datetime.fromtimestamp(START/1000,timezone.utc)
    result=reader.bind_market(raw,'BTC',START,observed)
    assert result['outcomes']=={'UP':'123','DOWN':'456'}
    assert result['anchor'] is None
    for field,value in [('resolutionSource','https://wrong.example'),('version','unknown'),
                        ('clobTokenIds','["123","123"]'),('conditionId','0xwrong'),
                        ('slug','btc-updown-5m-1'),('description','no rule')]:
        with pytest.raises(ValueError): reader.bind_market({**raw,field:value},'BTC',START,observed)


def test_shared_reads_keep_prices_on_failure_and_expire_round_binding(monkeypatch):
    clock=[(START+1000)/1000]
    monkeypatch.setattr(reader.time,'time',lambda:clock[0])
    reads=[]
    def markets(now):
        return {'BTC':reader.bind_market(market(),'BTC',START,datetime.fromtimestamp(START/1000,timezone.utc))}
    def prices(now):
        reads.append(now)
        return {'source':'market-data','schemaVersion':1,'assets':{'BTC':{'prices':[point(START+500)],'opening':point()}}}
    monkeypatch.setattr(reader,'read_markets',markets)
    monkeypatch.setattr(reader,'read_prices',prices)
    class Books:
        async def cached_read(self,*args):
            return {'conditionId':'0x'+'a'*64,'generatedAt':'2026-11-03T02:30:01Z','upstreamCompleteness':'UNKNOWN',
                    'yes':{'tokenId':'123','bookStatus':'available','bestBid':.4,'bestAsk':.5,'eventAt':None}}
    async def run():
        service=reader.ReferenceReader(Books())
        values=await asyncio.gather(*(service.read() for _ in range(20)))
        assert len(reads)==1
        btc=values[0]['assets']['BTC']
        assert btc['market']['anchor']['condition_id']=='0x'+'a'*64
        assert btc['quotes']['123']['quality']=='snapshot'
        assert btc['quotes']['123']['upstream_completeness']=='UNKNOWN'
        def fail(now): raise OSError('offline')
        monkeypatch.setattr(reader,'read_prices',fail)
        service.expires=0
        failed=await service.read()
        assert failed['assets']['BTC']['prices']==btc['prices']
        assert failed['assets']['BTC']['read_error']=='TWAP_READ_FAILED'
        assert failed['assets']['BTC']['market']['anchor']==btc['market']['anchor']
        clock[0]+=300
        monkeypatch.setattr(reader,'read_markets',lambda now:{})
        service.expires=0
        next_round=await service.read()
        assert next_round['assets']['BTC']['market'] is None
        assert next_round['assets']['BTC']['quotes']=={}
    asyncio.run(run())
