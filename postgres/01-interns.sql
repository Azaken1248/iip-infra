-- Release 1 schema for the Database Adapter (see docs/03-data-model.md §4.1).
-- record_id is the idempotency key for `interns.created` redelivery;
-- intern_id is unique so a future ON CONFLICT (intern_id) DO UPDATE
-- (Release 5, intern.updated) has a natural conflict target.

CREATE TABLE IF NOT EXISTS interns (
    record_id    UUID PRIMARY KEY,
    intern_id    VARCHAR(64) NOT NULL UNIQUE,
    first_name   VARCHAR(255) NOT NULL,
    last_name    VARCHAR(255) NOT NULL,
    email        VARCHAR(255) NOT NULL,
    college      VARCHAR(255) NOT NULL,
    department   VARCHAR(255) NOT NULL,
    mentor       VARCHAR(255),
    start_date   DATE NOT NULL,
    status       VARCHAR(32) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ
);
