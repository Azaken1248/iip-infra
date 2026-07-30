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

# $1 = label, $2 = fixture path, then table:source-file pairs. Every repo that
# keeps a copy of a shared table gets a call; add a pair when one starts using
# another table.
check_fixture() {
	label="$1"
	fixture="$2"
	shift 2

	echo "== checking $label's SQL fixture against $INFRA/postgres"

	if [ ! -f "$fixture" ]; then
		fail "missing SQL fixture $fixture"
		return
	fi

	for pair in "$@"; do
		table="${pair%%:*}"
		source_file="$INFRA/postgres/${pair#*:}"

		real=$(normalized_table "$source_file" "$table")
		copy=$(normalized_table "$fixture" "$table")

		if [ -z "$real" ]; then
			fail "table '$table' not found in $source_file -- the gate is checking a table that moved"
		elif [ -z "$copy" ]; then
			fail "table '$table' is missing from $fixture -- $label's tests would run without it"
		elif [ "$real" != "$copy" ]; then
			fail "table '$table' in $fixture has drifted from $source_file"
		else
			echo "  ok  $label fixture: $table"
		fi
	done
}

check_fixture "db-adapter" "$REPOS/db-adapter/src/test/resources/init.sql" \
	"interns:01-interns.sql" "records:03-records.sql"

# The registry's own tables. It runs ddl-auto: validate against this copy, so a
# drifted one means a suite that passes against a schema the deployment does not
# have -- the same hazard as the adapters', one layer up.
check_fixture "contract-registry" "$REPOS/contract-registry/src/test/resources/registry-schema.sql" \
	"contracts:02-registry.sql" "adapter_attachments:02-registry.sql" \
	"adapter_types:05-adapter-types.sql"

# --- 0c. the generic adapter pattern (Phase 6.2) ------------------------------
#
# Architecture §6 calls its pipeline diagram "the acceptance checklist for any
# new adapter", and every adapter carries a copy of that pipeline rather than a
# dependency on a shared jar -- deliberately, since this platform has no
# artifact repository and UC-9's promise is that a new adapter type is a new
# service, not a new service plus a versioned dependency on ours.
#
# The cost of that choice is that the copies can drift, and a drifted copy is a
# reliability guarantee quietly lost in one adapter. So the copies are compared
# here, modulo the package name that is the only legitimate difference between
# them. Phase 6.10's acceptance suite checks that each adapter *behaves*
# correctly; this checks that they are still the same shape.

echo "== checking the generic adapter pipeline is identical across adapters"

pipeline_of() {
	# $1 = repo dir, $2 = package segment, $3 = class
	file="$REPOS/$1/src/main/java/com/iip/$2/pipeline/$3.java"
	if [ ! -f "$file" ]; then
		echo "__MISSING__$file"
		return
	fi
	sed "s/$2/ADAPTER/g" "$file"
}

# Every adapter type, including ones added after this check was written. A repo
# that is not checked out is skipped rather than failed: the adapters are
# separate repositories and nobody is obliged to have all of them.
ADAPTERS="db-adapter:dbadapter file-adapter:fileadapter webhook-adapter:webhookadapter"

for class in RecordPipeline RecordEnvelope IdempotencyGate TargetWriter; do
	reference=""
	reference_name=""

	for adapter in $ADAPTERS; do
		repo="${adapter%%:*}"
		pkg="${adapter#*:}"
		[ -d "$REPOS/$repo" ] || continue

		copy=$(pipeline_of "$repo" "$pkg" "$class")

		case "$copy" in
			*__MISSING__*)
				fail "pipeline class '$class' is missing from $repo -- every adapter type carries the whole shape"
				continue
				;;
		esac

		if [ -z "$reference" ]; then
			reference="$copy"
			reference_name="$repo"
		elif [ "$reference" != "$copy" ]; then
			fail "pipeline class '$class' has drifted between $reference_name and $repo -- one adapter's reliability guarantees are no longer the other's"
		fi
	done

	[ -n "$reference" ] && echo "  ok  pipeline: $class"
done

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
