"""One durable catalog, separate from capture journals and rebuildable projections."""
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from .wire import json_bytes

SCOPE_POLICY='open_binary_identity_closed_or_no_orderbook_excluded_v2'


class CatalogOwner:
    def __init__(self,root):
        root=Path(root);root.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(root/'catalog.sqlite',timeout=60)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS markets(market TEXT PRIMARY KEY,gamma_id TEXT,body TEXT,state TEXT NOT NULL,evidence TEXT,candidate TEXT);
          CREATE INDEX IF NOT EXISTS gamma_identity ON markets(gamma_id);
          CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS input_cursors(log_id TEXT PRIMARY KEY,byte_offset INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS changes(revision INTEGER PRIMARY KEY,observed_ns INTEGER NOT NULL,kind TEXT NOT NULL,market TEXT,evidence TEXT);
          CREATE TABLE IF NOT EXISTS token_owners(token TEXT PRIMARY KEY,market TEXT NOT NULL);
        ''')
        if not self.metadata('token_owners_initialized',False):
            with self.db:
                self.db.execute('BEGIN IMMEDIATE')
                self.db.execute("INSERT OR IGNORE INTO token_owners SELECT j.value,m.market FROM markets m,json_each(m.body,'$.assets') j WHERE m.body IS NOT NULL")
                self.set_metadata('token_owners_initialized',True)

    def close(self):self.db.close()

    def metadata(self,key,default=None):
        row=self.db.execute('SELECT value FROM metadata WHERE key=?',(key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_metadata(self,key,value):
        self.db.execute('INSERT OR REPLACE INTO metadata VALUES (?,?)',(key,json.dumps(value)))

    def changed(self,kind,market,evidence):
        revision=self.metadata('revision',0)+1
        self.set_metadata('revision',revision)
        self.db.execute('INSERT INTO changes VALUES (?,?,?,?,?)',(revision,time.time_ns(),kind,market,json.dumps(evidence)))

    def upsert(self,row,evidence):
        old=self.db.execute('SELECT body,state FROM markets WHERE market=?',(row['market'],)).fetchone()
        if old and old[1]=='TERMINAL':return False
        if old and old[1]=='QUARANTINED':return False
        if old and old[1]=='EXCLUDED_SCOPE' and evidence.get('source')!='GAMMA_KNOWN_ID_REVALIDATION':return False
        body=json.dumps(row,sort_keys=True)
        if old and old[0]:
            previous=json.loads(old[0])
            identity=lambda m:(m['assets'],m['version'],m['asset_id_kind'],m.get('outcomes'))
            if identity(previous)!=identity(row):
                self.db.execute("UPDATE markets SET state='QUARANTINED',candidate=?,evidence=? WHERE market=?",(body,json.dumps(evidence),row['market']))
                self.changed('IDENTITY_QUARANTINED',row['market'],evidence)
                return False
            if old[0]==body and old[1]=='ELIGIBLE':return False
        conflicts=[token for token in row['assets'] if (owner:=self.db.execute('SELECT market FROM token_owners WHERE token=?',(token,)).fetchone()) and owner[0]!=row['market']]
        if conflicts:
            evidence={**evidence,'reason':'TOKEN_OWNERSHIP_CONFLICT','conflicting_tokens':conflicts}
            self.db.execute('INSERT OR REPLACE INTO markets VALUES (?,?,?,?,?,?)',
                (row['market'],row['gamma_id'],body,'QUARANTINED',json.dumps(evidence),body))
            self.changed('TOKEN_OWNERSHIP_CONFLICT',row['market'],evidence)
            return False
        for token in row['assets']:self.db.execute('INSERT OR IGNORE INTO token_owners VALUES (?,?)',(token,row['market']))
        self.db.execute('INSERT OR REPLACE INTO markets VALUES (?,?,?,?,?,NULL)',
            (row['market'],row['gamma_id'],body,'ELIGIBLE',json.dumps(evidence)))
        self.changed('UPSERT',row['market'],evidence)
        return True

    def resolve(self,market,evidence):
        old=self.db.execute('SELECT state FROM markets WHERE market=?',(market,)).fetchone()
        if old and old[0]=='TERMINAL':return False
        self.db.execute("INSERT INTO markets(market,state,evidence) VALUES (?,'TERMINAL',?) ON CONFLICT(market) DO UPDATE SET state='TERMINAL',evidence=excluded.evidence",
            (market,json.dumps(evidence)))
        self.changed('TERMINAL',market,evidence)
        return True

    def apply_scan(self,scope):
        evidence={'source':scope.get('discovery_source'),'scan_id':scope.get('catalog_revision'),
            'observed_at':scope.get('sweep_end_ns'),'complete':scope.get('full_universe_sweep',False)}
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            for row in scope['markets']:self.upsert(row,evidence)
            for excluded in scope.get('excluded',[]):
                # Absence is never terminal evidence. An explicit returned row can
                # exclude according to the unchanged scope rule, without inventing
                # a resolved tombstone from a REST 404.
                if excluded.get('id') is None or not (excluded.get('closed') or excluded.get('enableOrderBook') is False):continue
                for market,state in self.db.execute('SELECT market,state FROM markets WHERE gamma_id=?',(str(excluded['id']),)).fetchall():
                    if state not in ('TERMINAL','EXCLUDED_SCOPE'):
                        self.db.execute("UPDATE markets SET state='EXCLUDED_SCOPE',evidence=? WHERE market=?",(json.dumps({**evidence,**excluded}),market))
                        self.changed('SCOPE_EXCLUDED',market,{**evidence,**excluded})
            if scope.get('full_universe_sweep'):
                self.set_metadata('last_complete_scan',evidence)
                self.set_metadata('census_members',[row['market'] for row in scope['markets']])
            self.set_metadata('last_observed_scan',evidence)
        return self.snapshot(scope)

    def snapshot(self,source=None):
        # Hold one SQLite read transaction across members, revision and evidence.
        # A second connection may otherwise commit between these SELECTs.
        if not self.db.in_transaction:
            self.db.execute('BEGIN')
            try:return self._snapshot(source)
            finally:self.db.rollback()
        return self._snapshot(source)

    def _snapshot(self,source=None):
        rows=self.db.execute("SELECT market,body,state,evidence,candidate FROM markets WHERE state IN ('ELIGIBLE','QUARANTINED') AND body IS NOT NULL ORDER BY market").fetchall()
        groups=[{**json.loads(body),'admission_state':state} for _,body,state,_,_ in rows]
        members=[(m['market'],m['version'],m['assets'],m['asset_id_kind']) for m in groups]
        digest=hashlib.sha256(json_bytes(members)).hexdigest()
        return {**(source or {}),'markets':groups,'catalog_revision':self.metadata('revision',0),
            'scope_policy_version':SCOPE_POLICY,'membership_digest':digest,'catalog_digest':digest,
            'scope_kind':'VERSIONED_DESIRED_UNIVERSE','last_complete_scan':self.metadata('last_complete_scan'),
            'terminal_evidence':[{'market':m,'evidence':json.loads(e)} for m,e in self.db.execute("SELECT market,evidence FROM markets WHERE state='TERMINAL'")],
            'scope_exclusions':[{'market':m,'evidence':json.loads(e)} for m,e in self.db.execute("SELECT market,evidence FROM markets WHERE state='EXCLUDED_SCOPE'")],
            'unadmitted_groups':[{'market':m,'reason':json.loads(e).get('reason','IDENTITY_MIGRATION_REQUIRED'),'evidence':json.loads(e),'candidate':json.loads(c)} for m,_,state,e,c in rows if state=='QUARANTINED'],
            'pending_validation':[{'market':m,'evidence':json.loads(e)} for m,e in self.db.execute("SELECT market,evidence FROM markets WHERE state='PENDING_VALIDATION'")]}

    def consume_lifecycle(self,event,evidence):
        market=event.get('market')
        if not isinstance(market,str):raise ValueError('LIFECYCLE_MARKET_ID_MISSING')
        if event.get('event_type')=='market_resolved':return self.resolve(market,evidence)
        if event.get('event_type')=='new_market':
            if self.db.execute('SELECT 1 FROM markets WHERE market=?',(market,)).fetchone():return False
            self.db.execute("INSERT INTO markets(market,gamma_id,state,evidence,candidate) VALUES (?,?,'PENDING_VALIDATION',?,?)",
                (market,str(event.get('id','')),json.dumps(evidence),json.dumps(event)))
            self.changed('PENDING_VALIDATION',market,evidence)
            return True
        return False
