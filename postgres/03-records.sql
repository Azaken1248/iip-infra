-- Release 5 Phase 5.1 -- the generic landing table (docs/03-data-model.md §4.0).
--
-- The answer to the one genuinely new question generalization raises: how does
-- an arbitrary payload reach Postgres with no redeploy and no service holding
-- a DDL grant? Not by CREATE TABLE per contract (that hands a service DDL
-- rights and the ALTER TABLE migrations that follow), and not by pure JSONB
-- (that gives up typed columns everywhere, including where a guarantee depends
-- on one). The hybrid below is typed exactly where a guarantee needs it and
-- opaque everywhere else -- the same rule the rest of the platform lives by.
--
-- Which is to say: the two typed columns here are not a modelling preference,
-- they are two specific guarantees written down as constraints.

CREATE TABLE IF NOT EXISTS records (
    -- Envelope. Typed and PK because ON CONFLICT (record_id) DO NOTHING *is*
    -- the idempotency guarantee -- the same one interns.record_id carries in
    -- shaped mode (§4.1). Generalizing the write path did not weaken it.
    record_id    UUID PRIMARY KEY,
    contract_id  TEXT NOT NULL,
    record_type  TEXT NOT NULL,

    -- Envelope. Typed and unique per contract because
    -- ON CONFLICT (contract_id, natural_key) DO UPDATE is the upsert target
    -- for update-style record types (Phase 5.5). In shaped mode the
    -- contract_id half is implied by the table itself; here it is explicit,
    -- because one table holds every contract.
    natural_key  TEXT NOT NULL,

    -- Per-contract, and deliberately opaque to the adapter. A forms contract
    -- with 150 questions and variable-length option arrays flattens into
    -- columns badly and stores as JSONB naturally -- for schemas like that
    -- this is the better fit, not a compromise.
    payload      JSONB NOT NULL,

    occurred_at  TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT records_natural_key_per_contract UNIQUE (contract_id, natural_key)
);

-- The adapter's read pattern is per-contract, and the UNIQUE constraint above
-- already indexes (contract_id, natural_key) leading with contract_id, so no
-- separate contract_id index is needed -- Postgres uses that one's prefix.

-- Fields a contract marks queryable: true become expression indexes over
-- payload, created per contract when the attachment is registered (Phase 5.6).
-- They are NOT declared here: which fields are queryable is contract data in
-- the registry, and freezing today's answer into an init script is exactly the
-- redeploy-driven coupling this release exists to remove. For reference, the
-- shape 5.6 generates is:
--
--   CREATE INDEX records_interns_status
--       ON records ((payload->>'status')) WHERE contract_id = 'interns';
