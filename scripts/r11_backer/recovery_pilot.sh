#!/usr/bin/env bash
set -euo pipefail

branch="codex/r11-cas-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
api="repos/$GITHUB_REPOSITORY/contents/.jarvis-test/r11-synthetic.json"
stage=$(gh api "$api?ref=$branch")
old_sha=$(jq -er '.sha' <<< "$stage")
state=$(jq -er '.content' <<< "$stage" | base64 -d)
jq -e '.remaining == 0 and (.owner == "alpha" or .owner == "beta" or .owner == "gamma") and (has("receipt") | not)' <<< "$state" >/dev/null

store() {
  local expected_sha="$1" value="$2" payload
  payload=$(printf '%s' "$value" | base64 -w0)
  gh api --method PUT "$api" -f branch="$branch" -f sha="$expected_sha" \
    -f content="$payload" -f message='Update synthetic recovery ledger' --jq .content.sha
}

require_stale_rejection() {
  local expected_sha="$1" value="$2"
  if store "$expected_sha" "$value" >"$RUNNER_TEMP/recovery-write" 2>"$RUNNER_TEMP/recovery-error"; then
    echo "unexpected acceptance of an obsolete ledger version"
    exit 1
  fi
  grep -Eq 'HTTP (409|422)' "$RUNNER_TEMP/recovery-error"
}

taken_over=$(jq -c '.owner = "recovery" | .generation = 2' <<< "$state")
takeover_sha=$(store "$old_sha" "$taken_over")
old_receipt=$(jq -c '.receipt = "late_original_writer"' <<< "$state")
require_stale_rejection "$old_sha" "$old_receipt"
echo 'obsolete_writer=STALE_REJECTED'

python3 -m scripts.r11_backer.drive_pilot --verify-only
committed=$(jq -c '.receipt = "synthetic_drive_readback_verified"' <<< "$taken_over")
receipt_sha=$(store "$takeover_sha" "$committed")
require_stale_rejection "$takeover_sha" "$committed"

result=$(gh api "$api?ref=$branch")
test "$(jq -er '.sha' <<< "$result")" = "$receipt_sha"
jq -er '.content' <<< "$result" | base64 -d | jq -e '.remaining == 0 and .generation == 2 and .owner == "recovery" and .receipt == "synthetic_drive_readback_verified"' >/dev/null
echo 'existing_drive_archive_recovered_once=PASS'
