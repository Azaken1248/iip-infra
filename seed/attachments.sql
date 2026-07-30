-- Release 5 Phase 5.8 -- the deployment's starting attachments.
--
-- An attachment is control-plane *data*, not schema: it says "contract X fans
-- out to adapter type Y, configured like this" (docs/03-data-model.md §1c). It
-- lives in a SQL seed here for exactly one release. Phase 6.6 adds
-- `POST /contracts/{id}/adapters` and Phase 6.8 puts it behind the UI, at which
-- point attaching a target is something a user does at runtime and this file
-- becomes the two rows a fresh deployment happens to start with.
--
-- The contracts themselves are seeded differently -- `contract-registry-init`
-- POSTs infra/contracts/*.json through the public API -- because that API
-- exists. This one does not yet, and inventing a private one for a single
-- release would be two APIs to remove later.
--
-- Deliberately NOT in infra/postgres/, which is mounted into
-- docker-entrypoint-initdb.d. Two reasons, and the first is fatal:
--
--   1. adapter_attachments.contract_id references contracts(contract_id), and
--      the contracts are inserted by contract-registry-init long after initdb
--      has finished. As an init script this would hit a foreign key violation
--      and take the whole Postgres container down with it.
--   2. Init scripts run only against an empty data volume, which is the trap
--      Release 4 already fell into once. `attachment-init` runs this on every
--      `up`, so an existing deployment picks up a new attachment without
--      anyone remembering to apply it by hand.
--
-- ON CONFLICT DO NOTHING throughout, which is what makes running it on every
-- `up` safe.

-- interns: shaped-table mode (docs/03-data-model.md §4.1).
--
-- The `columns` map is the mapping that was Java until Phase 5.7 -- target
-- column on the left, payload field on the right. `record_id` and `created_at`
-- are deliberately absent: the adapter always writes those from the envelope,
-- because ON CONFLICT (record_id) DO NOTHING is the platform's idempotency
-- guarantee rather than this contract's preference.
--
-- This contract is in shaped mode to prove the generic table is a default and
-- not a mandate -- and because interns had a typed table before the generic one
-- existed, so leaving it here also proves the release broke nothing.
INSERT INTO adapter_attachments (attachment_id, contract_id, adapter_type, config, enabled)
VALUES (
    '3f1c9b2e-6d7a-4c58-9f21-5b8e4a0d7c13',
    'interns',
    'postgres',
    '{
       "mode": "shaped",
       "table": "interns",
       "columns": {
         "intern_id":  "internId",
         "first_name": "firstName",
         "last_name":  "lastName",
         "email":      "email",
         "college":    "college",
         "department": "department",
         "mentor":     "mentor",
         "start_date": "startDate",
         "status":     "status"
       }
     }'::jsonb,
    true
)
ON CONFLICT DO NOTHING;

-- forms: the generic landing table (§4.0).
--
-- `mode` is stated even though generic is what an attachment gets by saying
-- nothing. This file is read by operators deciding what a deployment does, and
-- "the absence of a key means the default" is a fact about the adapter's code,
-- not something a reader of this file should have to know.
--
-- There is no column mapping, no table name, and nothing naming a forms field --
-- that is the release's whole claim. A contract reaches Postgres because a row
-- here says it should.
INSERT INTO adapter_attachments (attachment_id, contract_id, adapter_type, config, enabled)
VALUES (
    '8a2d5e14-3b96-4f07-8c1d-2e6f9b4a7d05',
    'forms',
    'postgres',
    '{"mode": "generic"}'::jsonb,
    true
)
ON CONFLICT DO NOTHING;

-- interns -> the csv adapter (Phase 6.3).
--
-- The column list that was a constant in CsvInternWriter until this phase, in
-- the same order, so interns.csv keeps the exact shape every downstream reader
-- of it has seen since Release 1. record_id and created_at are absent for the
-- same reason they are absent above: the adapter always writes those from the
-- envelope, first and last.
--
-- A list, not an object: config is stored in a jsonb column, and jsonb does not
-- preserve an object's key order -- it sorts keys by length then bytewise. As an
-- object this mapping came back rearranged and the adapter wrote rows that did
-- not match their own header.
--
-- No `path`, so it falls back to the adapter's configured output file --
-- which is what this deployment has always written, and what the Targets page
-- reads.
INSERT INTO adapter_attachments (attachment_id, contract_id, adapter_type, config, enabled)
VALUES (
    'c4e7a1b8-9f30-4d62-8a15-7b2c6e0d3f94',
    'interns',
    'csv',
    '{
       "columns": [
         {"header": "intern_id",  "field": "internId"},
         {"header": "first_name", "field": "firstName"},
         {"header": "last_name",  "field": "lastName"},
         {"header": "email",      "field": "email"},
         {"header": "college",    "field": "college"},
         {"header": "department", "field": "department"},
         {"header": "mentor",     "field": "mentor"},
         {"header": "start_date", "field": "startDate"},
         {"header": "status",     "field": "status"}
       ]
     }'::jsonb,
    true
)
ON CONFLICT DO NOTHING;
