#!/usr/bin/env bash
# Load OpenFlare's own example deployment TOMLs through the real config
# loaders (the `deploy_toml` integration tests in each component crate).
#
# `VRT_DEPLOY_TOML_DIRS` looks for files named literally `<component>.toml`,
# while the examples are named `platform-<component>.toml` and
# `standalone-<component>.toml`. Materialize one directory per family with
# the looked-up name so both deployment modes are exercised side by side:
# openflare then validates the examples it documents, not only a consumer's
# kustomize output.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

copy_family() {
  local family="$1" prefix="$2" f comp
  for f in examples/"${prefix}"-*.toml; do
    [ -e "$f" ] || continue
    comp="$(basename "$f")"
    comp="${comp#"${prefix}"-}"
    case "$comp" in
      vrt-host.toml | vrt-provider.toml | vrt-sql.toml | cell-controller.toml) ;;
      *) continue ;;
    esac
    mkdir -p "$TMP/$family"
    cp "$f" "$TMP/$family/$comp"
  done
}

copy_family platform platform
copy_family standalone standalone

# Fail closed: an empty tree would make the shared helper panic with a
# less useful message.
if ! find "$TMP" -name '*.toml' | grep -q .; then
  echo "no example deployment TOMLs found under examples/" >&2
  exit 1
fi

VRT_DEPLOY_TOML_DIRS="$TMP/platform:$TMP/standalone" cargo test --offline \
  -p vrt-host --test deploy_toml \
  -p vrt-provider --test deploy_toml \
  -p vrt-sql --test deploy_toml \
  -p cell-controller --test deploy_toml
