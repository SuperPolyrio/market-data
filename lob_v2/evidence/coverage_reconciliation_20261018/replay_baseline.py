"""Replay retained production RAW frames through the replacement-baseline cut.

Run with PYTHONPATH=lob_v2/.vendor312:lob_v2 .venv/bin/python THIS_FILE.
No production/network access: replay journals use a temporary directory.
"""
import asyncio
import hashlib
import json
from pathlib import Path
import tempfile

from lobv2.book import state_view
from lobv2.core import Plant
from lobv2.journal import Journal
from lobv2.wire import decode

HERE=Path(__file__).parent


async def main():
    sample=json.loads((HERE/'baseline-production-sample.json').read_bytes())
    result=[]
    with tempfile.TemporaryDirectory(prefix='lob-baseline-replay-') as directory:
        journal=Journal(Path(directory),'replay-Q-0087','capture')
        plant=Plant(journal,{'markets':[sample['market']]},1)
        connection=sample['connection']
        plant.connect(0,0,connection,1,[sample['market']])
        plant.written(0,0,connection,sample['market']['assets'],'offline-production-replay')
        for i,entry in enumerate(sample['records']):
            frame=(HERE/entry['file']).read_bytes()
            assert hashlib.sha256(frame).hexdigest()==entry['sha256']
            native=decode(frame)
            before=state_view(plant.committed.get(sample['market']['market'],{}))
            rec=await plant.ingest_async(0,0,connection,native.body['recv_seq'],native.raw)
            updated=[s for s in plant.states.values() if s.get('baseline_offset')==rec.offset]
            assert updated
            for state in updated:
                assert not state['baseline_durable'] and not state['ready']
                assert state['baseline_capture_durable_ref'] is None
            assert state_view(plant.committed.get(sample['market']['market'],{}))==before
            await journal.sync()
            for state in updated:
                assert state['baseline_durable']
                assert state['baseline_capture_durable_ref']['record_sequence']==rec.offset
                assert state['baseline_capture_durable_ref']['log_generation']==journal.stream
            result.append({'production_offset':native.offset,'raw_sha256':hashlib.sha256(native.raw).hexdigest(),
                'updated_tokens':[s['token'] for s in updated],
                'previous_committed_book_preserved_before_sync':True,
                'new_baseline_receipt_matches_after_sync':True})
        await journal.close()
    receipt={'status':'PASS','sample_type':'ACTUAL_ARCHIVED_PRODUCTION_FRAMES_OFFLINE_REPLAY',
        'live_coverage_claim':False,'results':result}
    (HERE/'baseline-replay-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps(receipt,indent=2))


if __name__=='__main__':asyncio.run(main())
