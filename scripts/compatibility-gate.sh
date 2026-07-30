#!/bin/sh
# Phase 4.10 -- the compatibility gate.
#
# Runs two checks and fails the build if either does:
#
#   1. The ENVELOPE check. infra/schemas/envelope.json is submitted to the
#      Schema Registry's compatibility endpoint for subject iip.envelope-value.
#      A change that would stop a deployed adapter reading data written by a
#      deployed source service is rejected here.
#
#   2. The CONTRACT check, replayed over EVERY registered contract. Each
#      infra/contracts/*.json is submitted to the Contract Registry's
#      POST /contracts/{id}/compatibility -- the same rule the control-plane
#      API enforces at write time (Phase 4.9), asked as a question.
#
# Why both, when 4.9 already guards the API: the API stops a bad *edit*; this
# stops a bad *rule* or a bad *file* from being merged. They fail at different
# times for different people. An operator editing a contract in the UI hits
# 4.9; a developer changing envelope.json in a pull request hits this.
#
# Also checked here, and easy to miss otherwise: each service keeps a copy of
# envelope.json under src/test/resources for its tests to run without a
# registry. Those copies are fixtures, not sources of truth, and a fixture
# that has drifted from the real schema makes a test suite that passes while
# production would not.
#
# Usage:
#   scripts/compatibility-gate.sh
#
# Environment:
#   SCHEMA_REGISTRY_URL     default http://localhost:8085
#   CONTRACT_REGISTRY_URL   default http://localhost:8083
#
# Requires: curl, jq. Both registries must be reachable -- a gate that skips
# itself when a dependency is down is not a gate.
set -eu

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8085}"
CONTRACT_REGISTRY_URL="${CONTRACT_REGISTRY_URL:-http://localhost:8083}"
SUBJECT="${ENVELOPE_SUBJECT:-iip.envelope-value}"

HERE="$(cd "$(dirname "$0")" && pwd)"
# Overridable so the gate can be pointed at a prepared tree -- which is how
# infra/e2e-tests proves it actually blocks a deliberately-breaking change.
# A gate nobody has ever seen fail is a gate nobody knows works.
INFRA="${IIP_INFRA_DIR:-$(dirname "$HERE")}"
REPOS="${IIP_REPOS_DIR:-$(dirname "$INFRA")}"
ENVELOPE="$INFRA/schemas/envelope.json"
CONTENT_TYPE='Content-Type: application/vnd.schemaregistry.v1+json'

failures=0

fail() {
	echo "FAIL: $1" >&2
	failures=$((failures + 1))
}

# --- 0. the fixtures that stand in for the real schema in tests --------------

echo "== checking that each service's test fixture matches $ENVELOPE"
for copy in \
	"$REPOS/source-service/src/test/resources/schemas/envelope.json" \
	"$REPOS/db-adapter/src/test/resources/schemas/envelope.json" \
	"$REPOS/file-adapter/src/test/resources/schemas/envelope.json"; do

	if [ ! -f "$copy" ]; then
		fail "missing test fixture $copy"
	elif ! diff -q "$ENVELOPE" "$copy" >/dev/null; then
		fail "$copy has drifted from $ENVELOPE -- tests would pass against a schema production does not use"
	else
		echo "  ok  $(echo "$copy" | sed "s|$REPOS/||")"
	fi
done

# --- 0b. the SQL fixtures that stand in for the real schema in tests ---------
#
# Same hazard as the envelope fixtures above, one layer down. db-adapter keeps a
# copy of the tables it reads and writes under src/test/resources/init.sql so
# its Testcontainers Postgres has a schema without this repo being present.
#
# Not a whole-file diff, because that copy is deliberately a *subset*: it holds
# interns and records and pointedly not the contract registry's own tables,
# which this adapter reads over HTTP rather than by querying. So each shared
# table is compared on its own, normalized -- comments stripped, whitespace
# collapsed -- which is the part that has to agree.

normalized_table() {
	# $1 = file, $2 = table name. Empty output means "not found in this file",
	# which the caller must treat as a failure rather than a match, or a typo'd
	# table name would silently compare "" against "".
	sed -n "/CREATE TABLE IF NOT EXISTS $2 (/,/^);/p" "$1" \
		| sed 's/--.*$//' \
		| tr -s ' \t\n' ' ' \
		| sed 's/ *$//'
}

echo "== checking db-adapter's SQL fixture against $INFRA/postgres"
FIXTURE="$REPOS/db-adapter/src/test/resources/init.sql"

if [ ! -f "$FIXTURE" ]; then
	fail "missing SQL fixture $FIXTURE"
else
	# table:source-file pairs. Add a row when an adapter starts using a table.
	for pair in "interns:01-interns.sql" "records:03-records.sql"; do
		table="${pair%%:*}"
		source_file="$INFRA/postgres/${pair#*:}"

		real=$(normalized_table "$source_file" "$table")
		copy=$(normalized_table "$FIXTURE" "$table")

		if [ -z "$real" ]; then
			fail "table '$table' not found in $source_file -- the gate is checking a table that moved"
		elif [ -z "$copy" ]; then
			fail "table '$table' is missing from $FIXTURE -- db-adapter's tests would run without it"
		elif [ "$real" != "$copy" ]; then
			fail "table '$table' in $FIXTURE has drifted from $source_file"
		else
			echo "  ok  db-adapter fixture: $table"
		fi
	done
fi

# --- 1. the envelope ---------------------------------------------------------

echo "== checking $ENVELOPE against subject $SUBJECT"
jq -Rs '{schemaType: "JSON", schema: .}' <"$ENVELOPE" >/tmp/envelope-check.json

# The registry answers 404 when the subject has no versions yet. That is not a
# compatibility failure -- there is nothing to be incompatible with -- but it
# must be distinguished from a compatible verdict rather than lumped in with
# one, or a gate pointed at an empty registry would pass everything.
status=$(curl -sS -o /tmp/envelope-verdict.json -w '%{http_code}' \
	-X POST "$SCHEMA_REGISTRY_URL/compatibility/subjects/$SUBJECT/versions/latest?verbose=true" \
	-H "$CONTENT_TYPE" \
	--data-binary @/tmp/envelope-check.json)

case "$status" in
	200)
		if [ "$(jq -r '.is_compatible' </tmp/envelope-verdict.json)" = "true" ]; then
			echo "  ok  envelope is backward-compatible with the registered version"
		else
			fail "envelope.json is NOT backward-compatible with $SUBJECT: $(jq -c '.messages // .' </tmp/envelope-verdict.json)"
		fi
		;;
	404)
		echo "  --  $SUBJECT has no registered version yet; nothing to compare against"
		;;
	*)
		fail "the schema registry at $SCHEMA_REGISTRY_URL answered $status: $(cat /tmp/envelope-verdict.json)"
		;;
esac

# --- 2. every registered contract -------------------------------------------

echo "== replaying the contract check over $INFRA/contracts"
for contract in "$INFRA"/contracts/*.json; do
	[ -e "$contract" ] || continue
	contract_id=$(jq -r '.contractId' <"$contract")

	status=$(curl -sS -o /tmp/contract-verdict.json -w '%{http_code}' \
		-X POST "$CONTRACT_REGISTRY_URL/contracts/$contract_id/compatibility" \
		-H 'Content-Type: application/json' \
		--data-binary @"$contract")

	if [ "$status" != "200" ]; then
		fail "the contract registry answered $status for '$contract_id': $(cat /tmp/contract-verdict.json)"
		continue
	fi

	if [ "$(jq -r '.compatible' </tmp/contract-verdict.json)" = "true" ]; then
		echo "  ok  $contract_id"
	else
		fail "'$contract_id' is NOT backward-compatible with the registered version: $(jq -c '.problems' </tmp/contract-verdict.json)"
	fi
done

# --- verdict ----------------------------------------------------------------

echo
if [ "$failures" -eq 0 ]; then
	echo "compatibility gate passed"
	exit 0
fi

echo "compatibility gate FAILED with $failures problem(s)" >&2
echo "An incompatible change fails the build, not production (Data Model 5)." >&2
exit 1
