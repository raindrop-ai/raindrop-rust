#!/usr/bin/env bash
#
# Re-sync the Contract v1 golden corpus from raindrop-workshop into this
# crate's vendored copy. The corpus is authored in raindrop-workshop;
# this script just mirrors the latest state into `contract-fixtures/v1/`.
#
# Usage:
#
#   ./scripts/sync_contract_fixtures.sh
#       Look for raindrop-workshop next to this repo on disk:
#         ../raindrop-workshop/contract/fixtures/v1/
#
#   RAINDROP_WORKSHOP_DIR=/path/to/raindrop-workshop \
#       ./scripts/sync_contract_fixtures.sh
#       Sync from an explicit raindrop-workshop checkout.
#
# After running, `git status` will show the diff inside
# `contract-fixtures/v1/`. Review it, run `cargo test --test
# contract_corpus`, and commit the result on the same PR as any matching
# update to `src/contract/v1/`.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${ROOT}/contract-fixtures/v1"

WORKSHOP_DIR="${RAINDROP_WORKSHOP_DIR:-${ROOT}/../raindrop-workshop}"
SOURCE="${WORKSHOP_DIR}/contract/fixtures/v1"

if [[ ! -d "${SOURCE}" ]]; then
  echo "error: source corpus not found at ${SOURCE}" >&2
  echo "       set RAINDROP_WORKSHOP_DIR to the raindrop-workshop checkout root" >&2
  exit 1
fi

echo "syncing ${SOURCE}/  →  ${DEST}/"

mkdir -p "${DEST}"

# `--delete` keeps the destination an exact mirror — fixtures removed
# upstream are removed here too. The README at contract-fixtures/README.md
# is one level up from v1/, so it is not touched.
rsync -a --delete \
  --exclude '.DS_Store' \
  "${SOURCE}/" "${DEST}/"

echo
echo "done. next steps:"
echo "  git status                                # review the diff"
echo "  cargo test --test contract_corpus        # confirm parity"
echo "  cargo clippy --all-targets -- -D warnings"
