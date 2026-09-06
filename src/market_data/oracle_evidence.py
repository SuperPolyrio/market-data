"""Current, fail-closed settlement evidence for every stored market.

The SQL lives in the installed Python package so consumers and operators use
the same view definition. Installation is explicit; reads never change data.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Callable, Iterable, Mapping


COMBO_CALCULATION_VERSION = "0x572cd48cce93b2e58f1cc0253a7fdd4b4952a9c2"

_MODULE_CTES = r"""
module_epochs AS (
    SELECT DISTINCT ON (module_address) module_address,implementation,evidence
    FROM (
        SELECT module_address,lower(implementation) AS implementation,
               block_number,1 AS priority,0::bigint AS log_index,
               jsonb_build_object('table','oracle.module_implementation_observations',
                   'module_address',module_address,'implementation',lower(implementation),
                   'block_number',block_number,'block_hash',block_hash,
                   'observed_at',observed_at,'source',source) AS evidence
        FROM oracle.module_implementation_observations
        WHERE block_number>0 AND block_hash ~ '^0x[0-9a-f]{64}$' AND observed_at IS NOT NULL
        UNION ALL
        SELECT module_address,lower(payload_json#>>'{args,implementation}'),
               block_number,0,log_index,
               jsonb_build_object('table','oracle.module_events','event_id',id,
                   'module_address',module_address,'implementation',lower(payload_json#>>'{args,implementation}'),
                   'event_name',event_name,'tx_hash',tx_hash,'log_index',log_index,
                   'block_number',block_number,'event_time',event_time)
        FROM oracle.module_events WHERE event_name='Upgraded'
    ) candidates
    ORDER BY module_address,block_number DESC,priority DESC,log_index DESC
), combo_ids AS (
    SELECT condition_id, count(DISTINCT legs_json) AS definitions,
           (array_agg(id ORDER BY block_number, log_index, id))[1] AS preparation_id
    FROM oracle.module_events
    WHERE module_address='0x30000034706c7d8e12009dab006be20000c031a8'
      AND event_name='CombinatorialConditionPrepared'
      %(combo_filter)s
    GROUP BY condition_id
), combo_definitions AS (
    SELECT e.*, c.definitions
    FROM combo_ids c JOIN oracle.module_events e ON e.id=c.preparation_id
), combo_legs AS (
    SELECT d.condition_id, leg.ordinality AS ordinal,
           leg.value->>'condition_id' AS leg_condition_id,
           CASE WHEN leg.value->>'outcome_index' IN ('0','1')
                THEN (leg.value->>'outcome_index')::integer END AS outcome_index,
           CASE WHEN leg.value->>'position_id' ~ '^[1-9][0-9]{0,77}$'
                THEN (leg.value->>'position_id')::numeric END AS position_id,
           position.token_base AS expected_position_base,
           COALESCE(
               (leg.value->>'module_id'='1' AND leg.value->>'condition_id' ~ '^0x01[0-9a-f]{60}$')
               OR (leg.value->>'module_id'='2' AND leg.value->>'condition_id' ~ '^0x02[0-9a-f]{60}$'),
               FALSE
           ) AS module_valid
    FROM combo_definitions d
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(d.legs_json)='array' THEN d.legs_json ELSE '[]'::jsonb END
    ) WITH ORDINALITY AS leg(value, ordinality)
    LEFT JOIN LATERAL (
        SELECT sum(get_byte(decode(
                   CASE WHEN leg.value->>'condition_id' ~ '^0x0[12][0-9a-f]{60}$'
                        THEN substring(leg.value->>'condition_id',3) ELSE repeat('00',31) END,
                   'hex'),byte_index)::numeric * power(256::numeric,31-byte_index)) AS token_base
        FROM generate_series(0,30) AS bytes(byte_index)
    ) position ON TRUE
), %(native_targets)s native_ids AS (
    SELECT condition_id, count(*) AS resolution_count,
           (array_agg(id ORDER BY block_number DESC, log_index DESC, id DESC))[1] AS resolution_id
    FROM %(native_source)s
    WHERE event_name='ConditionResolved' AND event_status='settle'
      AND (
          (module_address='0x1000008dd9001b968442c1000017eae6e0da00ba' AND condition_id ~ '^0x01[0-9a-f]{60}$')
          OR (module_address='0x200000900045e3b6259600682756002200028933' AND condition_id ~ '^0x02[0-9a-f]{60}$')
      )
      %(native_filter)s
    GROUP BY condition_id
), native_parsed AS (
    SELECT e.*, n.resolution_count,
           CASE WHEN e.payouts_json::text ~ '^\[\s*(0|[1-9][0-9]{0,6})\s*,\s*(0|[1-9][0-9]{0,6})\s*\]$'
                THEN ARRAY[(e.payouts_json->>0)::numeric, (e.payouts_json->>1)::numeric]
           END AS numerators
    FROM native_ids n JOIN oracle.module_events e ON e.id=n.resolution_id
), native_results AS (
    SELECT n.*, ep.implementation,ep.evidence AS implementation_evidence,
           CASE WHEN resolution_count<>1 THEN 'CONFLICTING_MODULE_SETTLEMENTS'
                WHEN numerators IS NULL OR numerators[1]+numerators[2]<>1000000
                    THEN 'INVALID_MODULE_PAYOUT'
                WHEN n.tx_hash IS NULL OR n.tx_hash !~ '^0x[0-9a-f]{64}$'
                    OR n.block_number IS NULL OR n.block_number<=0
                    OR n.log_index IS NULL OR n.log_index<0 OR n.event_time IS NULL
                    THEN 'INCOMPLETE_MODULE_PROVENANCE'
                WHEN ep.implementation IS NULL THEN 'MODULE_IMPLEMENTATION_UNVERIFIED'
                WHEN ep.implementation<>CASE n.module_address
                    WHEN '0x1000008dd9001b968442c1000017eae6e0da00ba'
                        THEN '0x492fec596ec347459e1ebe30b9245eb3b49b1bba'
                    ELSE '0xa61e7ca374f721d5b9fd5b0fee6fb90f27d448d7' END
                    THEN 'MODULE_IMPLEMENTATION_UNSUPPORTED'
                ELSE 'READY' END AS readiness,
           jsonb_build_object('table','oracle.module_events','event_id',n.id,
               'event_name',n.event_name,'module_address',n.module_address,
               'condition_id',n.condition_id,'tx_hash',n.tx_hash,'log_index',n.log_index,
               'block_number',n.block_number,'event_time',n.event_time
           ) AS supporting_event
    FROM native_parsed n
    LEFT JOIN module_epochs ep ON ep.module_address=n.module_address
), ordered_legs AS (
    SELECT l.*, lag(position_id) OVER (PARTITION BY condition_id ORDER BY ordinal) AS previous_position
    FROM combo_legs l
), combo_stats AS (
    SELECT l.condition_id, count(*) AS leg_count,
           count(DISTINCT l.leg_condition_id)=count(*)
               AND bool_and(l.module_valid AND l.outcome_index IS NOT NULL
                   AND l.position_id IS NOT NULL AND l.position_id<power(2::numeric,256)
                   AND l.position_id=l.expected_position_base+l.outcome_index
                   AND (l.ordinal=1 OR l.position_id>l.previous_position)) AS legs_valid,
           count(*) FILTER (WHERE n.id IS NULL) AS missing_results,
           bool_or(n.readiness='CONFLICTING_MODULE_SETTLEMENTS') AS conflicting_results,
           bool_or(n.readiness='INVALID_MODULE_PAYOUT') AS invalid_results,
           bool_or(n.readiness='INCOMPLETE_MODULE_PROVENANCE') AS incomplete_provenance,
           bool_or(ep.implementation IS NULL) AS implementation_unverified,
           bool_or(ep.implementation<>CASE left(l.leg_condition_id,4)
               WHEN '0x01' THEN '0x492fec596ec347459e1ebe30b9245eb3b49b1bba'
               ELSE '0xa61e7ca374f721d5b9fd5b0fee6fb90f27d448d7' END) AS implementation_unsupported,
           COALESCE(jsonb_agg(DISTINCT ep.evidence) FILTER (WHERE ep.evidence IS NOT NULL),
               '[]'::jsonb) AS implementation_evidence,
           bool_or(n.readiness='READY' AND n.numerators[l.outcome_index+1]=0) AS terminal_zero,
           array_agg(n.numerators[l.outcome_index+1] ORDER BY l.ordinal) AS leg_numerators,
           (array_agg(n.id ORDER BY n.block_number, n.log_index, n.id)
               FILTER (WHERE n.readiness='READY' AND n.numerators[l.outcome_index+1]=0))[1] AS first_zero_id,
           (array_agg(n.id ORDER BY n.block_number DESC, n.log_index DESC, n.id DESC)
               FILTER (WHERE n.readiness='READY'))[1] AS last_resolution_id,
           COALESCE(jsonb_agg(n.supporting_event ORDER BY l.ordinal)
               FILTER (WHERE n.id IS NOT NULL), '[]'::jsonb) AS supporting_events
    FROM ordered_legs l
    LEFT JOIN native_results n ON n.condition_id=l.leg_condition_id
    LEFT JOIN module_epochs ep ON ep.module_address=CASE left(l.leg_condition_id,4)
        WHEN '0x01' THEN '0x1000008dd9001b968442c1000017eae6e0da00ba'
        WHEN '0x02' THEN '0x200000900045e3b6259600682756002200028933' END
    GROUP BY l.condition_id
), combo_results AS (
    SELECT d.condition_id, d.id AS preparation_id, d.legs_json,
           CASE WHEN (d.block_number,d.log_index) >= (w.block_number,w.log_index)
                THEN d.id ELSE w.id END AS id,
           CASE WHEN (d.block_number,d.log_index) >= (w.block_number,w.log_index)
                THEN d.tx_hash ELSE w.tx_hash END AS tx_hash,
           CASE WHEN (d.block_number,d.log_index) >= (w.block_number,w.log_index)
                THEN d.log_index ELSE w.log_index END AS log_index,
           greatest(d.block_number,w.block_number) AS block_number,
           greatest(d.event_time,w.event_time) AS event_time,
           greatest(d.created_at,w.created_at) AS created_at,
           d.module_address,ep.implementation,
           jsonb_build_array(ep.evidence) || COALESCE(s.implementation_evidence,'[]'::jsonb)
               AS implementation_evidence,
           COALESCE(s.terminal_zero,FALSE) AS terminal_zero,
           CASE WHEN s.terminal_zero THEN ARRAY[0::numeric,1e36::numeric]
                WHEN s.missing_results=0 THEN ARRAY[f.down,1e36::numeric-f.up]
           END AS numerators,
           CASE WHEN d.definitions<>1 THEN 'CONFLICTING_COMBO_PREPARATION'
                WHEN COALESCE(s.leg_count,0) NOT BETWEEN 1 AND 50 OR NOT COALESCE(s.legs_valid,FALSE)
                    THEN 'INVALID_COMBO_PREPARATION'
                WHEN s.conflicting_results THEN 'CONFLICTING_MODULE_SETTLEMENTS'
                WHEN s.invalid_results THEN 'INVALID_MODULE_PAYOUT'
                WHEN s.incomplete_provenance OR d.tx_hash IS NULL OR d.tx_hash !~ '^0x[0-9a-f]{64}$'
                    OR d.block_number IS NULL OR d.block_number<=0 OR d.log_index IS NULL OR d.log_index<0
                    OR d.event_time IS NULL THEN 'INCOMPLETE_MODULE_PROVENANCE'
                WHEN ep.implementation IS NULL OR s.implementation_unverified
                    THEN 'MODULE_IMPLEMENTATION_UNVERIFIED'
                WHEN ep.implementation<>'0x572cd48cce93b2e58f1cc0253a7fdd4b4952a9c2'
                    OR s.implementation_unsupported THEN 'MODULE_IMPLEMENTATION_UNSUPPORTED'
                WHEN s.terminal_zero OR s.missing_results=0 THEN 'READY'
                ELSE 'MISSING_COMBO_LEG_RESULT' END AS readiness,
           jsonb_build_array(jsonb_build_object('table','oracle.module_events','event_id',d.id,
               'event_name',d.event_name,'module_address',d.module_address,
               'condition_id',d.condition_id,'tx_hash',d.tx_hash,'log_index',d.log_index,
               'block_number',d.block_number,'event_time',d.event_time))
               || COALESCE(s.supporting_events,'[]'::jsonb) AS supporting_events
    FROM combo_definitions d
    LEFT JOIN module_epochs ep ON ep.module_address=d.module_address
    LEFT JOIN combo_stats s ON s.condition_id=d.condition_id
    LEFT JOIN native_results w ON w.id=CASE WHEN s.terminal_zero THEN s.first_zero_id ELSE s.last_resolution_id END
    LEFT JOIN LATERAL (
        WITH RECURSIVE factors(ordinal,down,up) AS (
            VALUES (0,1e36::numeric,1e36::numeric)
            UNION ALL
            SELECT ordinal+1, div(down*s.leg_numerators[ordinal+1],1000000),
                   div(up*s.leg_numerators[ordinal+1]+999999,1000000)
            FROM factors WHERE ordinal<cardinality(s.leg_numerators)
        ) SELECT down,up FROM factors ORDER BY ordinal DESC LIMIT 1
    ) f ON NOT COALESCE(s.terminal_zero,FALSE)
), module_facts AS (
    SELECT condition_id,id,tx_hash,log_index,block_number,event_time,created_at,module_address,
           numerators,1000000::numeric AS denominator,readiness,
           'V2_CONDITION_RESOLUTION'::text AS settlement_basis,
           jsonb_build_array(supporting_event) AS supporting_events,
           NULL::jsonb AS combo_legs, implementation AS calculation_version,
           FALSE AS terminal_zero,jsonb_build_array(implementation_evidence) AS implementation_evidence
    FROM native_results
    UNION ALL
    SELECT condition_id,id,tx_hash,log_index,block_number,event_time,created_at,module_address,
           numerators,1e36::numeric,readiness,'COMBO_LEG_RESOLUTIONS',
           supporting_events,legs_json,implementation,terminal_zero,implementation_evidence
    FROM combo_results
)
"""


_SETTLEMENT_QUERY = r"""
WITH %(requested)s ctf AS (
    SELECT condition_id,
           count(*) FILTER (WHERE event_status='request') AS request_count,
           count(*) FILTER (WHERE event_status='settle') AS settle_count,
           (array_agg(id ORDER BY block_number DESC, log_index DESC, id DESC)
               FILTER (WHERE event_status='settle'))[1] AS settlement_event_id
    FROM oracle.oracle_events
    WHERE source_oracle='0x4d97dcd97ec945f40cf65f87097ace5ea0476045'
      AND condition_id ~ '^0x[0-9a-f]{64}$'
      %(ctf_filter)s
    GROUP BY condition_id
), uma AS (
    SELECT market_id,
           count(*) FILTER (WHERE event_status='request') AS request_count,
           count(*) FILTER (WHERE event_status='propose') AS proposal_count,
           count(*) FILTER (WHERE event_status='dispute') AS dispute_count,
           count(*) FILTER (WHERE event_status='settle') AS settle_count,
           (array_agg(id ORDER BY block_number DESC, log_index DESC, id DESC)
               FILTER (WHERE event_status='settle'))[1] AS adjudication_event_id
    FROM oracle.oracle_events
    WHERE market_id IS NOT NULL
      %(uma_filter)s
      AND source_oracle IN (
          '0xbb1a8db2d4350976a11cdfa60a1d43f97710da49',
          '0x2c0367a9db231ddebd88a94b4f6461a6e47c58b1',
          '0xee3afe347d5c74317041e2618c49534daf887c24'
      )
    GROUP BY market_id
), token_map AS (
    SELECT market_id, count(*) AS token_count,
           min(outcome_index) AS first_slot, max(outcome_index) AS last_slot,
           count(DISTINCT outcome_index) AS distinct_slots,
           count(DISTINCT token_id) AS distinct_tokens,
           bool_and(CASE WHEN token_id ~ '^[1-9][0-9]{0,77}$'
               THEN token_id::numeric < power(2::numeric, 256)
               ELSE FALSE END) AS token_ids_valid,
           bool_and(condition_id IS NOT NULL AND condition_id <> '') AS conditions_present,
           max(CASE WHEN outcome_index=0 AND token_id ~ '^[1-9][0-9]{0,77}$'
               THEN token_id::numeric END) AS slot_zero_token,
           max(CASE WHEN outcome_index=1 AND token_id ~ '^[1-9][0-9]{0,77}$'
               THEN token_id::numeric END) AS slot_one_token,
           min(condition_id) AS first_condition, max(condition_id) AS last_condition,
           jsonb_agg(jsonb_build_object(
               'token_id', token_id, 'outcome_index', outcome_index,
               'outcome', outcome, 'condition_id', condition_id
           ) ORDER BY outcome_index, token_id) AS tokens
    FROM core.market_tokens
    %(token_filter)s
    GROUP BY market_id
), parsed AS (
    SELECT oe.*,
           CASE WHEN oe.payout ~
               '^\s*\[\s*(0|[1-9][0-9]{0,77})(\s*,\s*(0|[1-9][0-9]{0,77}))+\s*\]\s*$'
           THEN regexp_split_to_array(
               trim(both '[]' from btrim(oe.payout)), '\s*,\s*'
           )::numeric[] END AS payout_numerators
    FROM oracle.oracle_events oe
    WHERE oe.source_oracle='0x4d97dcd97ec945f40cf65f87097ace5ea0476045'
      AND oe.event_status='settle'
), validated AS (
    SELECT parsed.*, amounts.payout_denominator,
           COALESCE(
               cardinality(parsed.payout_numerators) BETWEEN 2 AND 256
               AND amounts.smallest >= 0
               AND amounts.largest < power(2::numeric, 256)
               AND amounts.payout_denominator > 0
               AND amounts.payout_denominator < power(2::numeric, 256),
               FALSE
           ) AS payout_valid
    FROM parsed
    LEFT JOIN LATERAL (
        SELECT sum(value) AS payout_denominator,
               min(value) AS smallest, max(value) AS largest
        FROM unnest(parsed.payout_numerators) AS item(value)
    ) amounts ON TRUE
), %(module_ctes)s, evidence AS (
    SELECT m.id AS market_id, m.gamma_market_id, m.identity_kind, m.slug,
           m.condition_id, m.question_id, m.active, m.closed, m.end_date,
           COALESCE(c.request_count, 0) AS ctf_request_count,
           COALESCE(c.settle_count, 0) AS ctf_settle_count,
           COALESCE(v.id,f.id) AS settlement_event_id,
           COALESCE(v.tx_hash,f.tx_hash) AS settlement_tx_hash,
           COALESCE(v.log_index,f.log_index) AS settlement_log_index,
           COALESCE(v.block_number,f.block_number) AS settlement_block_number,
           COALESCE(v.event_time,f.event_time) AS settlement_event_time,
           COALESCE(v.created_at,f.created_at) AS settlement_stored_at,
           COALESCE(v.source_oracle,f.module_address) AS settlement_contract,
           v.source_adapter AS condition_oracle,
           v.adapter_question_id AS condition_question_id,
           COALESCE(v.payout,to_json(f.numerators)::text) AS raw_payout,
           COALESCE(v.payout_numerators,f.numerators) AS payout_numerators,
           COALESCE(v.payout_denominator,f.denominator) AS payout_denominator,
           COALESCE(v.payout_valid, f.readiness='READY', FALSE) AS payout_valid,
           COALESCE(
               (v.tx_hash ~ '^0x[0-9a-f]{64}$'
               AND v.source_adapter ~ '^0x[0-9a-f]{40}$'
               AND v.adapter_question_id ~ '^0x[0-9a-f]{64}$'
               AND v.block_number > 0 AND v.log_index >= 0
               AND v.event_time IS NOT NULL),
               (f.tx_hash ~ '^0x[0-9a-f]{64}$' AND f.block_number>0
                AND f.log_index>=0 AND f.event_time IS NOT NULL),
               FALSE
           ) AS event_provenance_valid,
           COALESCE(
               tm.token_count=cardinality(COALESCE(v.payout_numerators,f.numerators))
               AND tm.first_slot=0 AND tm.last_slot=tm.token_count-1
               AND tm.distinct_slots=tm.token_count
               AND tm.distinct_tokens=tm.token_count AND tm.token_ids_valid
               AND tm.conditions_present
               AND tm.first_condition=m.condition_id
               AND tm.last_condition=m.condition_id
               AND (f.condition_id IS NULL OR
                    (tm.slot_zero_token=nt.token_base AND tm.slot_one_token=nt.token_base+1)),
               FALSE
           ) AS token_mapping_valid,
           COALESCE(tm.tokens, '[]'::jsonb) AS tokens,
           COALESCE(u.request_count, 0) AS uma_request_count,
           COALESCE(u.proposal_count, 0) AS uma_proposal_count,
           COALESCE(u.dispute_count, 0) AS uma_dispute_count,
           COALESCE(u.settle_count, 0) AS uma_settle_count,
           ue.id AS uma_adjudication_event_id,
           ue.settled_price AS uma_adjudicated_price,
           ue.tx_hash AS uma_adjudication_tx_hash,
           ue.log_index AS uma_adjudication_log_index,
           ue.block_number AS uma_adjudication_block_number,
           ue.event_time AS uma_adjudication_event_time,
           CASE WHEN v.id IS NOT NULL THEN 'oracle.oracle_events'
                WHEN f.id IS NOT NULL THEN 'oracle.module_events' END AS settlement_event_table,
           CASE WHEN v.id IS NOT NULL THEN 'CTF_CONDITION_RESOLUTION'
                ELSE f.settlement_basis END AS settlement_basis,
           f.readiness AS module_readiness, f.combo_legs,
           f.supporting_events,
           COALESCE(f.calculation_version,'ctf_payout_vector') AS calculation_version,
           COALESCE(f.terminal_zero,FALSE) AS combo_terminal_zero,
           f.implementation_evidence AS module_implementation_evidence
    FROM %(market_source)s m
    LEFT JOIN ctf c ON c.condition_id=m.condition_id
    LEFT JOIN validated v ON v.id=c.settlement_event_id
    LEFT JOIN module_facts f ON f.condition_id=m.condition_id
    LEFT JOIN LATERAL (
        SELECT sum(get_byte(decode(substring(m.condition_id,3),'hex'),byte_index)::numeric
                   * power(256::numeric,31-byte_index)) AS token_base
        FROM generate_series(0,30) AS bytes(byte_index)
        WHERE f.readiness='READY' AND m.condition_id ~ '^0x0[123][0-9a-f]{60}$'
    ) nt ON TRUE
    LEFT JOIN token_map tm ON tm.market_id=m.id
    LEFT JOIN uma u ON u.market_id=m.id
    LEFT JOIN oracle.oracle_events ue ON ue.id=u.adjudication_event_id
), classified AS (
    SELECT evidence.*,
           CASE
               WHEN condition_id ~ '^0x0[123][0-9a-f]{60}$' THEN
                   CASE WHEN module_readiness='READY' AND NOT token_mapping_valid THEN 'TOKEN_MAPPING_CONFLICT'
                        WHEN module_readiness IS NOT NULL THEN module_readiness
                        WHEN left(condition_id,4)='0x03' THEN 'MISSING_COMBO_PREPARATION'
                        WHEN active AND NOT closed AND (end_date IS NULL OR end_date>CURRENT_TIMESTAMP)
                            THEN 'PENDING'
                        ELSE 'MISSING_MODULE_SETTLEMENT' END
               WHEN condition_id !~ '^0x[0-9a-f]{64}$' OR condition_id IS NULL
                   THEN 'UNSUPPORTED_MARKET_IDENTITY'
               WHEN ctf_settle_count > 1 THEN 'CONFLICTING_CTF_SETTLEMENTS'
               WHEN settlement_event_id IS NOT NULL AND NOT payout_valid
                   THEN 'INVALID_CTF_PAYOUT'
               WHEN settlement_event_id IS NOT NULL AND NOT event_provenance_valid
                   THEN 'INCOMPLETE_CTF_PROVENANCE'
               WHEN settlement_event_id IS NOT NULL AND NOT token_mapping_valid
                   THEN 'TOKEN_MAPPING_CONFLICT'
               WHEN settlement_event_id IS NOT NULL THEN 'READY'
               WHEN active AND NOT closed AND uma_settle_count=0
                    AND (end_date IS NULL OR end_date > CURRENT_TIMESTAMP)
                   THEN 'PENDING'
               ELSE 'MISSING_CTF_SETTLEMENT'
           END AS settlement_readiness
    FROM evidence
)
SELECT %(public_columns)s, settlement_readiness,
       settlement_readiness='READY' AS settlement_ready,
       CASE WHEN settlement_readiness='READY' THEN (
           SELECT jsonb_agg(jsonb_build_object(
               'token_id', token->>'token_id',
               'outcome_index', (token->>'outcome_index')::integer,
               'outcome', token->>'outcome',
               'numerator', payout_numerators[(token->>'outcome_index')::integer+1]::text,
               'denominator', payout_denominator::text
           ) ORDER BY (token->>'outcome_index')::integer)
           FROM jsonb_array_elements(tokens) AS item(token)
       ) END AS payout_by_token,
       settlement_event_table, settlement_basis, module_readiness,
       combo_legs, supporting_events, calculation_version, combo_terminal_zero,
       module_implementation_evidence
FROM classified
"""


def settlement_query(*, scoped: bool = False) -> str:
    """Use one evidence definition for global coverage and bounded indexed reads."""
    return _SETTLEMENT_QUERY % {
        "requested": (
            "requested AS MATERIALIZED (SELECT id,gamma_market_id,identity_kind,slug,"
            "condition_id,question_id,active,closed,end_date FROM core.markets "
            "WHERE id=ANY(%s::bigint[]))," if scoped else ""
        ),
        "ctf_filter": (
            "AND condition_id IN (SELECT condition_id FROM requested)" if scoped else ""
        ),
        "uma_filter": "AND market_id IN (SELECT id FROM requested)" if scoped else "",
        "token_filter": "WHERE market_id IN (SELECT id FROM requested)" if scoped else "",
        "market_source": "requested" if scoped else "core.markets",
        "module_ctes": _MODULE_CTES % {
            "combo_filter": (
                "AND condition_id IN (SELECT condition_id FROM requested "
                "WHERE condition_id ~ '^0x03[0-9a-f]{60}$')" if scoped else ""
            ),
            "native_filter": "",
            "native_targets": (
                "native_targets AS MATERIALIZED (SELECT condition_id FROM requested "
                "WHERE condition_id ~ '^0x0[12][0-9a-f]{60}$' "
                "UNION SELECT leg_condition_id FROM combo_legs)," if scoped else ""
            ),
            # OFFSET 0 keeps this lookup parameterized. Otherwise PostgreSQL
            # can scan all module history even when a CTF batch has no targets.
            "native_source": (
                "(SELECT e.* FROM native_targets t CROSS JOIN LATERAL "
                "(SELECT * FROM oracle.module_events e WHERE e.condition_id=t.condition_id OFFSET 0) e) scoped_events"
                if scoped else "oracle.module_events"
            ),
        },
        # Retain the original view's column order so CREATE OR REPLACE is safe.
        "public_columns": ", ".join("""
            market_id gamma_market_id identity_kind slug condition_id question_id active closed end_date
            ctf_request_count ctf_settle_count settlement_event_id settlement_tx_hash settlement_log_index
            settlement_block_number settlement_event_time settlement_stored_at settlement_contract
            condition_oracle condition_question_id raw_payout payout_numerators payout_denominator
            payout_valid event_provenance_valid token_mapping_valid tokens
            uma_request_count uma_proposal_count uma_dispute_count uma_settle_count
            uma_adjudication_event_id uma_adjudicated_price uma_adjudication_tx_hash
            uma_adjudication_log_index uma_adjudication_block_number uma_adjudication_event_time
        """.split()),
    }


SETTLEMENT_VIEW_SQL = (
    "CREATE OR REPLACE VIEW oracle.market_settlement_evidence AS\n" + settlement_query()
)


def install_settlement_view(conn: Any) -> None:
    """Install the view in the caller's transaction; never commit implicitly."""
    conn.execute(SETTLEMENT_VIEW_SQL)


def read_market_evidence(conn: Any, market_ids: Iterable[int]) -> list[dict[str, Any]]:
    """Read existing IDs; callers must detect requested IDs absent from core.markets."""
    ids = sorted(set(int(value) for value in market_ids))
    if not ids:
        return []
    return [dict(row) for row in conn.execute(
        settlement_query(scoped=True) + " ORDER BY market_id", (ids,),
    ).fetchall()]


def coverage_summary(
    conn: Any, *, batch_size: int = 10_000,
    on_progress: Callable[[dict[str, int]], None] | None = None,
) -> list[dict[str, Any]]:
    """Return complete counts only after traversal; use a caller-owned repeatable-read snapshot.

    The global inventory exceeds four million rows. Scope aggregation to keyset
    batches instead of sorting every token and event in one oversized query.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    # LLVM compilation cost ~1.8 s per 10k batch in the production plan;
    # keep these bounded lookups interpreted within the caller's transaction.
    conn.execute("SET LOCAL jit=off")
    bounds = conn.execute("SELECT min(id) AS first_id,max(id) AS last_id FROM core.markets").fetchone()
    if bounds["first_id"] is None:
        return []
    last_id = int(bounds["first_id"])-1
    max_id = int(bounds["last_id"])
    totals: Counter[tuple[str, str]] = Counter()
    scanned = 0
    while last_id < max_id:
        ids = [int(row["id"]) for row in conn.execute(
            "SELECT id FROM core.markets WHERE id>%s AND id<=%s ORDER BY id LIMIT %s",
            (last_id, max_id, batch_size),
        ).fetchall()]
        if not ids:
            break
        for row in conn.execute(
            "SELECT identity_kind,settlement_readiness,count(*) AS markets FROM ("
            + settlement_query(scoped=True)
            + ") evidence GROUP BY identity_kind,settlement_readiness", (ids,),
        ).fetchall():
            totals[(str(row["identity_kind"]), str(row["settlement_readiness"]))] += int(row["markets"])
        scanned += len(ids)
        last_id = ids[-1]
        if on_progress is not None:
            on_progress({"scanned_markets": scanned, "last_market_id": last_id, "max_market_id": max_id})
    return [
        {"identity_kind": identity, "settlement_readiness": readiness, "markets": count}
        for (identity, readiness), count in sorted(totals.items())
    ]


def payout_for_amount(evidence: Mapping[str, Any], token_id: str, amount: int) -> int:
    """Calculate exact native integer payout, including combo directional rounding."""
    if isinstance(amount, bool) or not isinstance(amount, int) or not 0 <= amount < 2**256:
        raise ValueError("amount must be an integer uint256 in native token units")
    if evidence.get("settlement_readiness") != "READY":
        raise ValueError("settlement evidence is not ready")
    matches = [row for row in evidence.get("payout_by_token") or [] if row["token_id"] == str(token_id)]
    if len(matches) != 1:
        raise ValueError("token is absent from the proven payout mapping")
    numerator, denominator = int(matches[0]["numerator"]), int(matches[0]["denominator"])
    if not 0 <= numerator <= denominator or denominator <= 0:
        raise ValueError("invalid payout fraction")
    product = amount * numerator
    checked_product = product
    if evidence.get("settlement_basis") == "COMBO_LEG_RESOLUTIONS":
        slot = int(matches[0]["outcome_index"])
        if evidence.get("combo_terminal_zero"):
            return amount if slot == 1 else 0
        # Source Solady mulDiv/mulDivUp use a checked 256-bit product. For
        # position 1 the contract multiplies the UP factor before complementing.
        checked_product = amount * (denominator-numerator) if slot == 1 else product
    if checked_product >= 2**256:
        raise ValueError("native payout multiplication would overflow uint256")
    return product // denominator
