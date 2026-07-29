#!/bin/sh
# Registers the canonical envelope (Phase 4.7) with a Schema Registry.
#
# Run by schema-registry-init on every `docker compose up`, and by
# scripts/compatibility-gate.sh in CI. Both callers need the same three
# things -- set the subject's compatibility rule, escape the schema file into
# a registration body, POST it -- so they share this rather than each growing
# their own slightly different version of it.
#
# Registering an identical schema is a no-op that returns the existing id, so
# this is safe to re-run; registering an *incompatible* one is rejected by the
# registry with HTTP 409, which is the whole point of the exercise.
#
# Requires: curl, jq.
set -eu

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8085}"
SUBJECT="${ENVELOPE_SUBJECT:-iip.envelope-value}"
SCHEMA_FILE="${ENVELOPE_SCHEMA_FILE:-$(dirname "$0")/envelope.json}"
CONTENT_TYPE='Content-Type: application/vnd.schemaregistry.v1+json'

echo "waiting for the schema registry at $SCHEMA_REGISTRY_URL ..."
attempt=0
until curl -sf "$SCHEMA_REGISTRY_URL/subjects" >/dev/null; do
	attempt=$((attempt + 1))
	if [ "$attempt" -ge 60 ]; then
		echo "schema registry never became reachable" >&2
		exit 1
	fi
	sleep 2
done

# -R reads the file as raw text and -s slurps it whole, so jq emits it as one
# correctly-escaped JSON string. The registry's API carries a schema document
# as a string field, and escaping one by hand is a well-known way to register
# something subtly different from what is in the repo.
jq -Rs '{schemaType: "JSON", schema: .}' <"$SCHEMA_FILE" >/tmp/envelope-registration.json

echo "registering $SCHEMA_FILE as $SUBJECT"
curl -sS --fail-with-body -X POST "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions" \
	-H "$CONTENT_TYPE" \
	--data-binary @/tmp/envelope-registration.json
echo

# The per-subject rule is set after the first version exists, because some
# registry builds reject a config PUT for a subject they have never heard of.
# The window that leaves is closed from the other side: both docker-compose.yml
# and the e2e harness set the registry-wide default to BACKWARD, so version 1
# is governed by the right rule from the moment it lands, and this call makes
# the subject say so explicitly rather than inheriting it.
echo "setting $SUBJECT compatibility to BACKWARD"
curl -sS --fail-with-body -X PUT "$SCHEMA_REGISTRY_URL/config/$SUBJECT" \
	-H "$CONTENT_TYPE" \
	-d '{"compatibility":"BACKWARD"}'
echo

# Read it back. A compatibility rule that silently failed to apply is worse
# than no rule at all: every later check would pass and report nothing wrong.
effective=$(curl -sS "$SCHEMA_REGISTRY_URL/config/$SUBJECT" | jq -r '.compatibilityLevel // empty')
if [ "$effective" != "BACKWARD" ]; then
	echo "expected $SUBJECT to be BACKWARD, but the registry reports '${effective:-unset}'" >&2
	exit 1
fi

echo "envelope schema registered, $SUBJECT is BACKWARD"
