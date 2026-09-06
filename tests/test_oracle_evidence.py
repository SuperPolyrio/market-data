"""Run the SQL check against an explicitly selected disposable PostgreSQL container.

ORACLE_EVIDENCE_TEST_CONTAINER=codex-oracle-evidence-tests pytest -q tests/test_oracle_evidence.py
Never point this at a production container: the fixture creates its own database.
"""

from __future__ import annotations

import json
import os
import subprocess
import uuid

import psycopg
from psycopg.rows import dict_row
import pytest

from market_data.oracle_evidence import (
    SETTLEMENT_VIEW_SQL, coverage_summary, payout_for_amount, read_market_evidence, settlement_query,
)
from market_data.oracle_modules import combo_condition_id


def test_empty_inventory_is_a_read_without_a_database_call():
    assert read_market_evidence(object(), []) == []


def test_settlement_sql_preserves_inventory_and_rejects_incomplete_evidence():
    container = os.environ.get("ORACLE_EVIDENCE_TEST_CONTAINER")
    if not container:
        pytest.skip("requires an explicitly selected disposable PostgreSQL container")
    database = "oracle_evidence_test_" + uuid.uuid4().hex

    def execute(sql: str, db: str = database) -> str:
        return subprocess.run(
            ["docker", "exec", "-i", container, "psql", "-U", "postgres",
             "-d", db, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-At"],
            input=sql, text=True, check=True, capture_output=True,
        ).stdout.strip()

    execute(f"CREATE DATABASE {database}", "postgres")
    try:
        execute("""
            CREATE SCHEMA core; CREATE SCHEMA oracle;
            CREATE TABLE core.markets (
                id bigint PRIMARY KEY, gamma_market_id text, identity_kind text,
                slug text, condition_id text, question_id text, active boolean,
                closed boolean, end_date timestamptz
            );
            CREATE TABLE core.market_tokens (
                market_id bigint, token_id text, outcome_index integer,
                outcome text, condition_id text
            );
            CREATE TABLE oracle.oracle_events (
                id bigint PRIMARY KEY, condition_id text, market_id bigint,
                event_status text, source_oracle text, source_adapter text,
                adapter_question_id text, block_number bigint, log_index bigint,
                tx_hash text, event_time timestamptz, created_at timestamptz,
                settled_price text, payout text
            );
            CREATE TABLE oracle.module_events (
                id bigint PRIMARY KEY, tx_hash text, log_index bigint, block_number bigint,
                event_time timestamptz, created_at timestamptz, module_address text,
                condition_id text, event_status text, event_name text,
                payouts_json jsonb, legs_json jsonb, payload_json jsonb
            );
            CREATE TABLE oracle.module_implementation_observations (
                module_address text PRIMARY KEY, block_number bigint, block_hash text,
                implementation text, observed_at timestamptz, source text
            );
            INSERT INTO core.markets
            SELECT n, n::text, 'canonical_gamma', 'test-' || n,
                   '0x' || lpad(to_hex(n),64,'0'), '0x' || repeat('f',64),
                   FALSE, TRUE, '2020-01-01'::timestamptz
            FROM generate_series(1,17) AS item(n);
            UPDATE core.markets SET active=TRUE,closed=FALSE,end_date=NULL WHERE id=3;
            UPDATE core.markets SET identity_kind='official_combo',condition_id='0x03' || repeat('0',60) WHERE id=5;
            UPDATE core.markets SET condition_id='0xlegacy' WHERE id=6;
            INSERT INTO core.market_tokens
            SELECT id, (id*100+slot)::text, slot, 'position_' || slot, condition_id
            FROM core.markets CROSS JOIN generate_series(0,1) AS item(slot)
            WHERE id<>9;
            INSERT INTO oracle.oracle_events
            SELECT id, condition_id, NULL, 'settle',
                   '0x4d97dcd97ec945f40cf65f87097ace5ea0476045',
                   '0x' || repeat('e',40), question_id, 100, 3,
                   '0x' || lpad(to_hex(id),64,'0'),
                   '2026-01-01'::timestamptz, '2026-01-02'::timestamptz,
                   NULL, '[1,0]'
            FROM core.markets WHERE id IN (1,2,7,8,9,10,11,12,13,14,15,16,17);
            UPDATE oracle.oracle_events SET payout='[7,7]' WHERE id=2;
            UPDATE oracle.oracle_events SET payout='[0,0]' WHERE id=7;
            UPDATE oracle.oracle_events SET payout='broken JSON' WHERE id=8;
            UPDATE oracle.oracle_events SET event_time=NULL WHERE id=10;
            UPDATE oracle.oracle_events SET payout='[-1,2]' WHERE id=12;
            UPDATE oracle.oracle_events SET payout='[true,0]' WHERE id=13;
            UPDATE oracle.oracle_events SET payout='[1e0,0]' WHERE id=14;
            UPDATE oracle.oracle_events SET payout='[01,0]' WHERE id=15;
            UPDATE oracle.oracle_events SET payout='[' || power(2::numeric,256)::text || ',0]' WHERE id=16;
            UPDATE oracle.oracle_events SET payout='[' || (power(2::numeric,256)-1)::text || ',1]' WHERE id=17;
            INSERT INTO oracle.oracle_events
            SELECT 111, condition_id, NULL, event_status, source_oracle,
                   source_adapter, adapter_question_id, 101, log_index,
                   '0x' || repeat('a',64), event_time, created_at, settled_price, '[0,1]'
            FROM oracle.oracle_events WHERE id=11;
            INSERT INTO oracle.oracle_events
            SELECT 104, condition_id, id, 'settle',
                   '0xee3afe347d5c74317041e2618c49534daf887c24',
                   '0x' || repeat('e',40), question_id, 99, 2,
                   '0x' || repeat('b',64), '2026-01-01'::timestamptz,
                   '2026-01-02'::timestamptz, '1', '5000000000000000000'
            FROM core.markets WHERE id=4;
        """)
        execute(SETTLEMENT_VIEW_SQL)
        rows = json.loads(execute(
            "SELECT json_agg(row_to_json(e) ORDER BY market_id) "
            "FROM oracle.market_settlement_evidence e"
        ))
        assert len(rows) == 17
        by_id = {row["market_id"]: row for row in rows}
        assert {i: row["settlement_readiness"] for i, row in by_id.items()} == {
            1: "READY", 2: "READY", 3: "PENDING", 4: "MISSING_CTF_SETTLEMENT",
            5: "MISSING_COMBO_PREPARATION", 6: "UNSUPPORTED_MARKET_IDENTITY",
            7: "INVALID_CTF_PAYOUT", 8: "INVALID_CTF_PAYOUT",
            9: "TOKEN_MAPPING_CONFLICT", 10: "INCOMPLETE_CTF_PROVENANCE",
            11: "CONFLICTING_CTF_SETTLEMENTS", 12: "INVALID_CTF_PAYOUT",
            13: "INVALID_CTF_PAYOUT", 14: "INVALID_CTF_PAYOUT",
            15: "INVALID_CTF_PAYOUT", 16: "INVALID_CTF_PAYOUT",
            17: "INVALID_CTF_PAYOUT",
        }
        assert by_id[1]["ctf_request_count"] == 0
        assert by_id[1]["payout_by_token"] == [
            {"token_id": "100", "outcome_index": 0, "outcome": "position_0",
             "numerator": "1", "denominator": "1"},
            {"token_id": "101", "outcome_index": 1, "outcome": "position_1",
             "numerator": "0", "denominator": "1"},
        ]
        assert by_id[2]["payout_by_token"][0]["denominator"] == "14"
        assert by_id[4]["uma_adjudicated_price"] == "1"
        assert by_id[4]["uma_adjudication_event_id"] == 104
        assert not by_id[4]["settlement_ready"]
        assert all(row["payout_by_token"] is None for row in rows[2:])
        execute("""
            UPDATE oracle.oracle_events
            SET payout='[' || array_to_string(array_fill(1,ARRAY[257]),',') || ']' WHERE id=7
        """)
        assert execute(
            "SELECT settlement_readiness FROM oracle.market_settlement_evidence WHERE market_id=7"
        ) == "INVALID_CTF_PAYOUT"
        scoped = settlement_query(scoped=True).replace("%s::bigint[]", "ARRAY[1,4,5]::bigint[]")
        scoped_rows = json.loads(execute(
            "SELECT json_agg(row_to_json(e) ORDER BY market_id) FROM (" + scoped + ") e"
        ))
        assert scoped_rows == [by_id[i] for i in (1,4,5)]
        execute("UPDATE core.market_tokens SET condition_id=NULL WHERE token_id='100'")
        assert execute(
            "SELECT settlement_readiness FROM oracle.market_settlement_evidence WHERE market_id=1"
        ) == "TOKEN_MAPPING_CONFLICT"
        # A current view sees old event enrichment without relying on an ID watermark.
        execute("UPDATE oracle.oracle_events SET market_id=1 WHERE id=104")
        assert execute(
            "SELECT uma_settle_count FROM oracle.market_settlement_evidence WHERE market_id=1"
        ) == "1"

        # V2 conditions and combos use exact native IDs, even without Gamma metadata.
        conditions = {i: "0x01" + f"{i:060x}" for i in range(18,41)}
        conditions[19] = "0x02" + f"{19:060x}"
        conditions[30] = "0x04" + f"{30:060x}"
        def literal(value):
            return "'" + str(value).replace("'", "''") + "'"

        def leg(i, slot):
            return {"position_id": str(int(conditions[i],16)*256+slot),
                    "condition_id": conditions[i], "module_id": int(conditions[i][2:4],16),
                    "outcome_index": slot}

        combo_definitions = {
            20:[leg(18,0),leg(19,1)], 21:[leg(18,0),leg(27,0)],
            22:[leg(27,0),leg(19,0)], 23:[leg(18,0),leg(18,1)],
            24:[leg(18,0)], 31:[leg(i,0) for i in range(32,39)], 40:[leg(19,1)],
        }
        for i, legs in combo_definitions.items():
            conditions[i] = combo_condition_id([int(row["position_id"]) for row in legs])

        def event(i, market_id, name, payload, *, block=100):
            condition = conditions[market_id]
            module = {
                "01": "0x1000008dd9001b968442c1000017eae6e0da00ba",
                "02": "0x200000900045e3b6259600682756002200028933",
                "03": "0x30000034706c7d8e12009dab006be20000c031a8",
            }[condition[2:4]]
            values = [str(i),literal("0x"+f"{i:064x}"),"1",str(block),
                      "'2026-01-01'","'2026-01-02'",literal(module),literal(condition),
                      literal("settle" if name=="ConditionResolved" else "prepare"),literal(name),
                      literal(json.dumps(payload))+"::jsonb" if name=="ConditionResolved" else "NULL",
                      literal(json.dumps(payload))+"::jsonb" if name!="ConditionResolved" else "NULL",
                      "'{}'::jsonb"]
            return "INSERT INTO oracle.module_events VALUES ("+",".join(values)+");"

        statements = []
        implementations = {
            "0x1000008dd9001b968442c1000017eae6e0da00ba":"0x492fec596ec347459e1ebe30b9245eb3b49b1bba",
            "0x200000900045e3b6259600682756002200028933":"0xa61e7ca374f721d5b9fd5b0fee6fb90f27d448d7",
            "0x30000034706c7d8e12009dab006be20000c031a8":"0x572cd48cce93b2e58f1cc0253a7fdd4b4952a9c2",
        }
        for module, implementation in implementations.items():
            statements.append("INSERT INTO oracle.module_implementation_observations VALUES ("
                +literal(module)+",110,"+literal("0x"+"e"*64)+","+literal(implementation)
                +",'2026-01-03','rpc_storage_observation');")
        for i, condition in conditions.items():
            statements.append(
                "INSERT INTO core.markets VALUES ("+str(i)+",NULL,'protocol_structural','native',"
                +literal(condition)+",NULL,FALSE,TRUE,'2020-01-01');"
            )
            for slot in (0,1):
                statements.append("INSERT INTO core.market_tokens VALUES ("
                    +str(i)+","+literal(int(condition,16)*256+slot)+","+str(slot)+",'position_"
                    +str(slot)+"',"+literal(condition)+");")
        for i in (18,19,25,28,29,39,*range(32,39)):
            payouts = {18:[250000,750000],19:[0,1000000],25:[1,1],29:{}}.get(i,[333333,666667])
            statements.append(event(1000+i,i,"ConditionResolved",payouts))
        statements.append(event(3028,28,"ConditionResolved",[0,1000000],block=101))
        statements.append("UPDATE oracle.module_events SET tx_hash=NULL WHERE id=1039;")
        for i, legs in combo_definitions.items():
            statements.append(event(2000+i,i,"CombinatorialConditionPrepared",legs,block=90))
        statements.append(event(3024,24,"CombinatorialConditionPrepared",[leg(19,0)],block=91))
        statements.append("UPDATE core.markets SET active=TRUE,closed=FALSE,end_date=NULL WHERE id=26;")
        execute("\n".join(statements))
        native_rows = json.loads(execute(
            "SELECT json_agg(row_to_json(e) ORDER BY market_id) "
            "FROM oracle.market_settlement_evidence e WHERE market_id>=18"
        ))
        native_by_id = {row["market_id"]:row for row in native_rows}
        expected = {
            18:"READY",19:"READY",20:"READY",21:"MISSING_COMBO_LEG_RESULT",22:"READY",
            23:"INVALID_COMBO_PREPARATION",24:"CONFLICTING_COMBO_PREPARATION",
            25:"INVALID_MODULE_PAYOUT",26:"PENDING",27:"MISSING_MODULE_SETTLEMENT",
            28:"CONFLICTING_MODULE_SETTLEMENTS",29:"INVALID_MODULE_PAYOUT",
            30:"UNSUPPORTED_MARKET_IDENTITY",31:"READY",39:"INCOMPLETE_MODULE_PROVENANCE",40:"READY",
            **{i:"READY" for i in range(32,39)},
        }
        assert {i:r["settlement_readiness"] for i,r in native_by_id.items()} == expected
        for i, pair in ((18,[250000,750000]),(19,[0,1000000]),(20,[250000,750000]),(22,[0,1000000])):
            row = native_by_id[i]
            assert row["settlement_event_table"] == "oracle.module_events"
            assert [payout_for_amount(row, str(int(conditions[i],16)*256+s), 1000000) for s in (0,1)] == pair
        assert native_by_id[20]["settlement_basis"] == "COMBO_LEG_RESOLUTIONS"
        assert len(native_by_id[20]["supporting_events"]) == 3
        down = up = 10**36
        for _ in range(7):
            down = down*333333//1000000
            up = (up*333333+999999)//1000000
        assert down < up
        rounded = native_by_id[31]
        assert [int(p["numerator"]) for p in rounded["payout_by_token"]] == [down,10**36-up]
        for amount in (1,1000000,10**30):
            assert payout_for_amount(rounded,str(int(conditions[31],16)*256),amount) == amount*down//10**36
            assert payout_for_amount(rounded,str(int(conditions[31],16)*256+1),amount) == amount-(amount*up+10**36-1)//10**36
        with pytest.raises(ValueError,match="not ready"):
            payout_for_amount(native_by_id[21],str(int(conditions[21],16)*256),1000000)
        with pytest.raises(ValueError,match="absent"):
            payout_for_amount(rounded,"123",1)
        # Checked Solidity mulDiv uses the UP factor for the complemented slot,
        # while an actual zero leg returns before either multiplication.
        maximum = 2**256-1
        for slot in (0,1):
            assert payout_for_amount(native_by_id[22],str(int(conditions[22],16)*256+slot),maximum) == maximum*slot
            for market_id in (31,40):
                with pytest.raises(ValueError,match="overflow"):
                    payout_for_amount(native_by_id[market_id],str(int(conditions[market_id],16)*256+slot),maximum)
        no_amount = maximum//(10**36//4)
        assert payout_for_amount(native_by_id[20],str(int(conditions[20],16)*256+1),no_amount) == no_amount*3//4
        scoped = settlement_query(scoped=True).replace("%s::bigint[]", "ARRAY[20,21,22,31]::bigint[]")
        assert json.loads(execute(
            "SELECT json_agg(row_to_json(e) ORDER BY market_id) FROM ("+scoped+") e"
        )) == [native_by_id[i] for i in (20,21,22,31)]
        assert len(native_by_id[20]["module_implementation_evidence"]) == 3

        # Actual scoped API and complete keyset coverage agree with the view.
        # Imported inventories can contain zero or negative IDs; retain them.
        execute("""
            INSERT INTO core.markets (id,identity_kind,condition_id,active,closed)
            VALUES (-1,'legacy_ghost','legacy-negative',FALSE,TRUE),
                   (0,'legacy_ghost','legacy-zero',FALSE,TRUE)
        """)
        inspected = json.loads(subprocess.run(
            ["docker","inspect",container],check=True,capture_output=True,text=True,timeout=10,
        ).stdout)[0]
        host = next(iter(inspected["NetworkSettings"]["Networks"].values()))["IPAddress"]
        with psycopg.connect(host=host,dbname=database,user="postgres",connect_timeout=10,
                             row_factory=dict_row) as conn:
            conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            api_rows = read_market_evidence(conn,[20,21,22,31])
            assert [row["settlement_readiness"] for row in api_rows] == ["READY","MISSING_COMBO_LEG_RESULT","READY","READY"]
            progress = []
            summary = coverage_summary(conn,batch_size=7,on_progress=progress.append)
            assert summary == conn.execute("""
                SELECT identity_kind,settlement_readiness,count(*) AS markets
                FROM oracle.market_settlement_evidence GROUP BY 1,2 ORDER BY 1,2
            """).fetchall()
            assert progress[-1] == {"scanned_markets":42,"last_market_id":40,"max_market_id":40}

        def status(market_id):
            return execute("SELECT settlement_readiness FROM oracle.market_settlement_evidence "
                           "WHERE market_id="+str(market_id))

        execute("UPDATE core.market_tokens SET condition_id=NULL WHERE market_id=18 AND outcome_index=0")
        assert status(18) == "TOKEN_MAPPING_CONFLICT"
        execute("UPDATE oracle.module_events SET legs_json=jsonb_set(legs_json,'{0,position_id}','\"1\"') WHERE id=2020")
        assert status(20) == "INVALID_COMBO_PREPARATION"

        # A later unknown implementation invalidates both native and derived
        # evidence. An end-of-block storage observation supersedes older logs.
        for i, module in enumerate(implementations,9001):
            upgrade = ("INSERT INTO oracle.module_events (id,module_address,event_name,event_status,"
                "block_number,log_index,tx_hash,event_time,payload_json) VALUES ("
                +str(i)+","+literal(module)+",'Upgraded','upgrade',111,5,"+literal("0x"+f"{i:064x}")
                +",'2026-01-04',"+literal(json.dumps({"args":{"implementation":"0x"+"f"*40}}))+"::jsonb);")
            execute(upgrade)
            assert status(22) == "MODULE_IMPLEMENTATION_UNSUPPORTED"
            if i<9003:
                assert status(18 if i==9001 else 19) == "MODULE_IMPLEMENTATION_UNSUPPORTED"
            execute("UPDATE oracle.module_implementation_observations SET block_number=111 WHERE module_address="+literal(module))
            assert status(22) == "READY"
            execute("UPDATE oracle.module_implementation_observations SET block_number=110 WHERE module_address="+literal(module))
            assert status(22) == "MODULE_IMPLEMENTATION_UNSUPPORTED"
            execute("DELETE FROM oracle.module_events WHERE id="+str(i))
        execute("DELETE FROM oracle.module_implementation_observations")
        assert status(19) == "MODULE_IMPLEMENTATION_UNVERIFIED"
        assert status(22) == "MODULE_IMPLEMENTATION_UNVERIFIED"
    finally:
        execute(f"DROP DATABASE {database} WITH (FORCE)", "postgres")
