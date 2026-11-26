#!/usr/bin/env bash
# deploy-cloudflare.sh — deploy one staged conformance case to real
# Cloudflare via wrangler. Called by run.mjs --deploy-cmd for
# `--target cloudflare` runs; provisions bindings from the case's
# wrangler manifest (KV/R2/D1/queues) then `wrangler deploy`s.
#
# Runner env: CASE_DIR, CASE_ID, CASE_HOSTNAME (see deploy-standalone.sh).
# Required:  CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID
# Optional:  WRANGLER (default "wrangler"), CF_STATE (teardown ledger),
#            CF_SUBDOMAIN (workers.dev subdomain fallback)
#
# Prints VRT_CASE_HOSTNAME=<host> on stdout — the runner uses it as the
# per-case request Host.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec node "$HERE/cf-deploy.mjs"
