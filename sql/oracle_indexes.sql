-- Run outside a transaction so collectors can continue while this is built.
-- The full ancillary hex can exceed PostgreSQL's B-tree entry limit; the
-- lookup retains an exact ancillary equality predicate after this hash lookup.
CREATE INDEX CONCURRENTLY IF NOT EXISTS adapter_events_ancillary_hash_idx
ON oracle.adapter_events ((encode(digest(payload_json->>'ancillary_data', 'sha256'), 'hex')))
WHERE event_status = 'adapter_question_initialized';
