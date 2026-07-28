-- Release 4 Phase 4.1 -- the Contract Registry's tables (docs/03-data-model.md §1c).
--
-- This is the schema change that turns a contract from a file baked into the
-- source-service image into a row a service reads. Nothing else about a
-- contract changes: the `definition` column below holds the *same* shape the
-- YAML files hold today, which is what keeps Path A (contract mounted as
-- config into a per-contract pod) a deployment choice rather than a redesign
-- (AD-12).

CREATE TABLE IF NOT EXISTS contracts (
    contract_id     TEXT PRIMARY KEY,
    title           TEXT NOT NULL,
    schema_version  INT  NOT NULL,
    -- The full contract: naturalKey strategy, recordTypes, fields. Stored as
    -- one JSONB document rather than normalized into field/record-type tables
    -- because the registry never queries *inside* a definition -- it stores it
    -- and hands it back whole. Normalizing would buy nothing and would make
    -- the compatibility check (4.9) compare rows instead of documents.
    definition      JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT contracts_schema_version_positive CHECK (schema_version >= 1)
);

CREATE TABLE IF NOT EXISTS adapter_attachments (
    attachment_id   UUID PRIMARY KEY,
    contract_id     TEXT NOT NULL REFERENCES contracts(contract_id) ON DELETE CASCADE,
    adapter_type    TEXT NOT NULL,          -- 'postgres' | 'csv' | 'webhook'
    config          JSONB NOT NULL,         -- target-specific: table, endpoint, auth, mapping
    enabled         BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Data Model §1c declares UNIQUE (contract_id, adapter_type, config).
    -- Postgres has no btree operator class for `jsonb`, so that constraint is
    -- not directly buildable; the uniqueness is expressed over a deterministic
    -- digest of the config instead. Same guarantee -- "don't attach the same
    -- target to the same contract twice" -- reachable by an index. Generated
    -- rather than written by the caller so the digest cannot drift from the
    -- config it summarizes: there is no code path that updates one without
    -- the other.
    config_digest   TEXT GENERATED ALWAYS AS (md5(config::text)) STORED,

    CONSTRAINT adapter_attachments_unique_target
        UNIQUE (contract_id, adapter_type, config_digest)
);

-- The adapters' hot path (Release 5.2) is "which contracts am I attached to",
-- so this index leads with adapter_type rather than contract_id.
CREATE INDEX IF NOT EXISTS adapter_attachments_by_type
    ON adapter_attachments (adapter_type) WHERE enabled;
