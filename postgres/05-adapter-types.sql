-- Release 6 Phase 6.1 -- the adapter type catalog.
--
-- What a *type* is, as opposed to an attachment: `postgres` is a type,
-- "interns fans out to postgres, writing the interns table" is an attachment.
-- The catalog answers the question the control-plane UI has to ask before it
-- can offer [UC-14](02-use-cases.md)'s attach form -- "what kinds of target
-- exist, and what does each one need to be told?" -- and it is the piece that
-- makes [UC-9](02-use-cases.md) real: adding a type is deploying a service,
-- not editing this registry.
--
-- Rows are written by the adapters themselves. Each adapter ships a static
-- descriptor and PUTs it at startup, which keeps the declared config schema in
-- the same repository as the code that reads it -- the one arrangement where
-- the two cannot drift apart. A catalog maintained here instead would be a
-- second place to edit for every new type, and the first thing to go stale.

CREATE TABLE IF NOT EXISTS adapter_types (
    adapter_type    TEXT PRIMARY KEY,       -- 'postgres' | 'csv' | 'webhook'
    title           TEXT NOT NULL,          -- for humans, in a dropdown
    -- The full descriptor: title, description, and the config fields an
    -- attachment may supply. One JSONB document for the same reason
    -- contracts.definition is one -- the registry stores it and hands it back
    -- whole, and never queries inside it.
    descriptor      JSONB NOT NULL,
    -- When this type last announced itself. An adapter re-registers on every
    -- start, so a stale timestamp is how an operator spots a type whose
    -- service has been undeployed. Deliberately not used to hide the row:
    -- attachments referencing a type that is temporarily down must survive a
    -- restart of it.
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
