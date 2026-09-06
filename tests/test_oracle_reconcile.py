"""Exercise real reconciliation SQL in an explicitly selected disposable PG.

ORACLE_EVIDENCE_TEST_CONTAINER=codex-oracle-evidence-tests pytest -q tests/test_oracle_reconcile.py
"""

from __future__ import annotations

import json
import os
import subprocess
import uuid

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import pytest

from market_data import oracle
from market_data.storage import get_state, set_state


@pytest.fixture
def database():
    container = os.environ.get("ORACLE_EVIDENCE_TEST_CONTAINER")
    if not container:
        pytest.skip("requires an explicitly selected disposable PostgreSQL container")
    inspected = json.loads(subprocess.run(
        ["docker", "inspect", container], check=True, capture_output=True, text=True,
        timeout=10,
    ).stdout)[0]
    host = next(iter(inspected["NetworkSettings"]["Networks"].values()))["IPAddress"]
    name = "oracle_reconcile_test_" + uuid.uuid4().hex
    options = dict(host=host, user="postgres", connect_timeout=10,
                   options="-c statement_timeout=15000", row_factory=dict_row)
    with psycopg.connect(dbname="postgres", autocommit=True, **options) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            with psycopg.connect(dbname=name, **options) as conn:
                oracle._ensure_schema(conn)
                conn.execute("""
                    CREATE SCHEMA core;
                    CREATE TABLE core.markets (
                        id bigint PRIMARY KEY, gamma_market_id text, slug text,
                        title text, description text, question_id text, condition_id text
                    )
                """)
                yield conn
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def test_late_markets_reconcile_after_page_wrap_without_moving_chain_cursor(database):
    database.execute("""
        INSERT INTO oracle.oracle_events
            (tx_hash, log_index, block_number, event_status, source_oracle,
             external_market_id, question_id, condition_id)
        VALUES ('unknown', 0, 1, 'request', %s, 'unknown', '', ''),
               ('late-uma', 0, 2, 'request', %s, '555', 'q-uma', ''),
               ('late-ctf', 0, 3, 'request', %s, '', 'q-shared', 'c-ctf')
    """, (oracle.OO_V2, oracle.OO_V2, oracle.CTF))
    database.execute("""
        INSERT INTO oracle.adapter_events
            (tx_hash, log_index, block_number, event_time, event_status,
             source_adapter, question_id)
        VALUES ('late-adapter', 1, 2, NOW(), 'adapter_question_initialized', %s, 'q-uma')
    """, (next(iter(oracle.REQUESTERS_BY_ORACLE[oracle.OO_V2])),))
    set_state(database, oracle.UMA_LIVE_KEY, last_block=90_000_000)

    assert oracle.reconcile_market_links(database, limit=1)["oracle_linked"] == 0
    assert oracle.reconcile_market_links(database, limit=1)["oracle_linked"] == 0
    database.execute("""
        INSERT INTO core.markets (id, gamma_market_id, question_id, condition_id)
        VALUES (10, '555', 'q-uma', 'c-uma'), (20, '666', 'q-shared', 'wrong-condition')
    """)
    # CTF cannot attach to a different condition even when its question matches.
    assert oracle.reconcile_market_links(database, limit=1)["oracle_linked"] == 0
    assert oracle.reconcile_market_links(database, limit=1)["pass_complete"] == 1
    assert oracle.reconcile_market_links(database, limit=1)["oracle_linked"] == 0
    assert oracle.reconcile_market_links(database, limit=1)["oracle_linked"] == 1
    assert database.execute(
        "SELECT market_id, condition_id FROM oracle.oracle_events WHERE tx_hash='late-uma'"
    ).fetchone() == {"market_id": 10, "condition_id": "c-uma"}
    assert database.execute(
        "SELECT market_id FROM oracle.adapter_events WHERE tx_hash='late-adapter'"
    ).fetchone()["market_id"] == 10
    assert database.execute(
        "SELECT market_id FROM oracle.oracle_events WHERE tx_hash='late-ctf'"
    ).fetchone()["market_id"] is None

    database.execute("""
        INSERT INTO core.markets (id, gamma_market_id, question_id, condition_id)
        VALUES (30, '777', 'q-shared', 'c-ctf')
    """)
    assert oracle.reconcile_market_links(database, limit=1)["oracle_linked"] == 1
    assert database.execute(
        "SELECT market_id, condition_id FROM oracle.oracle_events WHERE tx_hash='late-ctf'"
    ).fetchone() == {"market_id": 30, "condition_id": "c-ctf"}
    assert get_state(database, oracle.UMA_LIVE_KEY)["last_block"] == 90_000_000
    assert database.execute(
        "SELECT market_id FROM oracle.oracle_events WHERE tx_hash='unknown'"
    ).fetchone()["market_id"] is None


def test_event_replay_preserves_existing_enrichment_and_identity(database):
    event = database.execute("""
        INSERT INTO oracle.oracle_events
            (tx_hash, log_index, block_number, event_status, market_id,
             external_market_id, market_title, matched_by,
             adapter_question_id, question_id, condition_id)
        VALUES ('replayed', 3, 4, 'request', 10, '555', 'Late market',
                'by_condition_id', 'q', 'q', 'c') RETURNING *
    """).fetchone()
    fields = ("external_market_id", "market_title", "matched_by",
              "adapter_question_id", "question_id", "condition_id")
    replay = {**event, **dict.fromkeys(fields, ""), "market_id": None}
    oracle._write_oracle_events(database, [replay])
    oracle._write_oracle_events(database, [replay])
    stored = database.execute(
        "SELECT * FROM oracle.oracle_events WHERE tx_hash='replayed' AND log_index=3"
    ).fetchall()
    assert len(stored) == 1
    assert stored[0]["id"] == event["id"]
    assert stored[0]["market_id"] == 10
    assert {key: stored[0][key] for key in fields} == {key: event[key] for key in fields}

    adapter = database.execute("""
        INSERT INTO oracle.adapter_events
            (tx_hash, log_index, block_number, event_time, event_status,
             source_adapter, question_id, market_id, condition_id)
        VALUES ('replayed-adapter', 3, 4, NOW(), 'adapter_question_initialized',
                'adapter', 'q', 10, 'c') RETURNING *
    """).fetchone()
    oracle._write_adapter_events(database, [{
        **adapter, "market_id": None, "condition_id": "", "payload_json": "{}",
    }])
    assert database.execute(
        "SELECT market_id, condition_id FROM oracle.adapter_events WHERE tx_hash='replayed-adapter'"
    ).fetchone() == {"market_id": 10, "condition_id": "c"}
