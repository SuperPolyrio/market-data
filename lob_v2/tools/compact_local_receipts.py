#!/usr/bin/env python3
"""Drop redundant footer indexes from completed local receipts, never archive objects."""
import argparse,json,sys,time
from pathlib import Path
PACKAGE=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(PACKAGE/(".vendor312" if sys.version_info >= (3,12) else ".vendor")),str(PACKAGE)]
from lobv2.journal import atomic_json

def compact(root):
    count=before=after=0
    for path in root.glob('*/journal/*.archive.json'):
        try:
            row=json.loads(path.read_bytes())
            # Eviction is the final receipt update. Active upload/eviction state
            # is left alone so no concurrent completion proof can be overwritten.
            if not row.get('eviction') or 'blocks' not in row['manifest']:continue
            original=path.stat().st_size
            row['manifest'].pop('blocks')
            row['footer_index_location']='IMMUTABLE_REMOTE_MANIFEST_AND_OBJECT_FOOTER'
            atomic_json(path,row)
            count+=1;before+=original;after+=path.stat().st_size
        except FileNotFoundError:pass
    return {'utc_ns':time.time_ns(),'receipts_compacted':count,'bytes_before':before,'bytes_after':after,'archive_record_bytes_removed':0}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,required=True);parser.add_argument('--report',type=Path,required=True);args=parser.parse_args();result=compact(args.root);atomic_json(args.report,result)
    with args.report.with_suffix('.jsonl').open('a') as out:out.write(json.dumps(result)+'\n')
    print(json.dumps(result))
