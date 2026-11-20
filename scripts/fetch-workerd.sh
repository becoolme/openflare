#!/usr/bin/env bash
# fetch-workerd.sh — materialize the workerd pinned by deploy/docker/workerd.lock.
#
# The lock is the pin of record. This script is the only place that knows how to
# turn it into bytes, so CI and local runs verify the same way.
#
#   scripts/fetch-workerd.sh --out DIR                    # binary + schema
#   scripts/fetch-workerd.sh --out DIR --capnp-only       # schema only (fast)
#   scripts/fetch-workerd.sh --out DIR --platform linux-arm64
#   scripts/fetch-workerd.sh --out DIR --lock other.lock  # alternate pin (testing)
#
# Supported lock sources (`source`, absent means "npm"):
#
#   npm             @cloudflare/workerd-<arch>@<version> tarball, plus the
#                   `workerd` meta package for workerd.capnp.
#                   Verifies `integrity` (sha512, when set) and `binarySha256`.
#
#   github-release  A GitHub release asset (usually .gz) from this project's
#                   workerd fork, plus the sibling workerd.capnp asset.
#                   Requires `binarySha256`; verifies it after decompression,
#                   and `archiveSha256` when set.
#
# Any unknown source, missing digest, or non-https URL fails the run: a lock
# that cannot be verified is not a pin.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCK="$ROOT/deploy/docker/workerd.lock"

OUT=""
PLATFORM=""
CAPNP_ONLY=0

usage() {
  sed -n '2,27p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --out) OUT="${2:-}"; shift 2 ;;
    --out=*) OUT="${1#*=}"; shift ;;
    --platform) PLATFORM="${2:-}"; shift 2 ;;
    --platform=*) PLATFORM="${1#*=}"; shift ;;
    --lock) LOCK="${2:-}"; shift 2 ;;
    --lock=*) LOCK="${1#*=}"; shift ;;
    --capnp-only) CAPNP_ONLY=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

[ -n "$OUT" ] || { echo "--out DIR is required" >&2; usage; }
[ -f "$LOCK" ] || { echo "lock file not found: $LOCK" >&2; exit 1; }

if [ -z "$PLATFORM" ]; then
  case "$(uname -s):$(uname -m)" in
    Linux:x86_64) PLATFORM=linux-x64 ;;
    Linux:aarch64|Linux:arm64) PLATFORM=linux-arm64 ;;
    *)
      # The schema is platform-independent; any artifact entry can supply it.
      if [ "$CAPNP_ONLY" = 1 ]; then
        PLATFORM=linux-x64
      else
        echo "unsupported platform $(uname -s)/$(uname -m); pass --platform" >&2
        exit 1
      fi
      ;;
  esac
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

require_sha256() {
  local file="$1" expected="$2" what="$3"
  local actual
  actual="$(sha256_of "$file")"
  if [ "$actual" != "$expected" ]; then
    echo "$what sha256 mismatch: got $actual, lock pins $expected" >&2
    exit 1
  fi
}

lock_get() {
  node -e '
    const fs = require("fs");
    const path = process.argv[1];
    const expr = process.argv[2];
    const lock = JSON.parse(fs.readFileSync(path, "utf8"));
    const value = Function("lock", "return " + expr)(lock);
    if (value === undefined || value === null) process.exit(1);
    process.stdout.write(String(value));
  ' "$LOCK" "$1"
}

SOURCE="$(lock_get '(lock.source || "npm")')"
VERSION="$(lock_get 'lock.version')"

case "$SOURCE" in
  npm)
    case "$PLATFORM" in
      linux-x64) NPM_ARCH="linux-64" ;;
      linux-arm64) NPM_ARCH="linux-arm64" ;;
      *) echo "npm source has no package for $PLATFORM" >&2; exit 1 ;;
    esac
    ARTIFACT="lock.artifacts[$(node -e 'process.stdout.write(JSON.stringify(process.argv[1]))' "$PLATFORM")]"
    INTEGRITY="$(lock_get "($ARTIFACT || {}).integrity || \"\"" )"
    EXPECTED_BIN="$(lock_get "($ARTIFACT || {}).binarySha256 || \"\"")"

    # The schema alone is a small download used by the Rust tests.
    ( cd "$TMP" && npm pack --silent "workerd@${VERSION}" >/dev/null )
    tar -xzf "$TMP/workerd-${VERSION}.tgz" -C "$TMP" package/workerd.capnp
    cp "$TMP/package/workerd.capnp" "$OUT/workerd.capnp"

    if [ "$CAPNP_ONLY" = 1 ]; then
      echo "fetch-workerd: $OUT/workerd.capnp (npm $VERSION)"
      exit 0
    fi

    [ -n "$EXPECTED_BIN" ] || {
      echo "lock has no artifacts.$PLATFORM.binarySha256 — refusing to fetch an unverifiable binary" >&2
      exit 1
    }

    ( cd "$TMP" && npm pack --silent "@cloudflare/workerd-${NPM_ARCH}@${VERSION}" >/dev/null )
    TGZ="$(echo "$TMP"/*.tgz | tr ' ' '\n' | grep -v "workerd-${VERSION}.tgz" | head -1)"
    [ -n "$TGZ" ] || { echo "no tarball downloaded for @cloudflare/workerd-${NPM_ARCH}" >&2; exit 1; }

    if [ -n "$INTEGRITY" ]; then
      GOT="sha512-$(openssl dgst -sha512 -binary "$TGZ" | openssl base64 -A)"
      [ "$GOT" = "$INTEGRITY" ] || {
        echo "tarball integrity mismatch: $GOT != $INTEGRITY" >&2
        exit 1
      }
    fi

    mkdir -p "$TMP/out"
    tar -xzf "$TGZ" -C "$TMP/out"
    BIN="$(find "$TMP/out" -type f -name workerd | head -1)"
    [ -n "$BIN" ] || { echo "no workerd binary inside $TGZ" >&2; exit 1; }
    require_sha256 "$BIN" "$EXPECTED_BIN" "workerd binary"
    install -m 0755 "$BIN" "$OUT/workerd"
    echo "fetch-workerd: $OUT/workerd (npm $VERSION, $PLATFORM)"
    ;;

  github-release)
    ARTIFACT="lock.artifacts[$(node -e 'process.stdout.write(JSON.stringify(process.argv[1]))' "$PLATFORM")]"
    URL="$(lock_get "($ARTIFACT || {}).url || \"\"")"
    ARCHIVE_SHA="$(lock_get "($ARTIFACT || {}).archiveSha256 || \"\"")"
    EXPECTED_BIN="$(lock_get "($ARTIFACT || {}).binarySha256 || \"\"")"
    CAPNP_URL="$(lock_get "($ARTIFACT || {}).capnpUrl || \"\"")"
    CAPNP_SHA="$(lock_get "($ARTIFACT || {}).capnpSha256 || \"\"" )"

    [ -n "$URL" ] || { echo "lock has no artifacts.$PLATFORM.url" >&2; exit 1; }
    case "$URL" in
      https://github.com/*|https://objects.githubusercontent.com/*) ;;
      *) echo "refusing non-GitHub https URL: $URL" >&2; exit 1 ;;
    esac
    [ -n "$EXPECTED_BIN" ] || {
      echo "github-release pins require artifacts.$PLATFORM.binarySha256" >&2
      exit 1
    }
    if [ -z "$CAPNP_URL" ]; then
      CAPNP_URL="${URL%/*}/workerd.capnp"
    fi
    case "$CAPNP_URL" in
      https://github.com/*|https://objects.githubusercontent.com/*) ;;
      *) echo "refusing non-GitHub https URL: $CAPNP_URL" >&2; exit 1 ;;
    esac

    curl -fsSL --retry 3 --retry-delay 2 -o "$TMP/capnp" "$CAPNP_URL" || {
      echo "failed to download workerd.capnp from $CAPNP_URL (release assets must ship it, or set capnpUrl)" >&2
      exit 1
    }
    if [ -n "$CAPNP_SHA" ]; then
      require_sha256 "$TMP/capnp" "$CAPNP_SHA" "workerd.capnp"
    fi
    cp "$TMP/capnp" "$OUT/workerd.capnp"

    if [ "$CAPNP_ONLY" = 1 ]; then
      echo "fetch-workerd: $OUT/workerd.capnp (github-release $VERSION)"
      exit 0
    fi

    curl -fsSL --retry 3 --retry-delay 2 -o "$TMP/asset" "$URL" || {
      echo "failed to download $URL" >&2
      exit 1
    }
    if [ -n "$ARCHIVE_SHA" ]; then
      require_sha256 "$TMP/asset" "$ARCHIVE_SHA" "release asset"
    fi
    case "$URL" in
      *.gz)
        gunzip -c "$TMP/asset" > "$TMP/workerd"
        ;;
      *)
        cp "$TMP/asset" "$TMP/workerd"
        ;;
    esac
    require_sha256 "$TMP/workerd" "$EXPECTED_BIN" "workerd binary"
    install -m 0755 "$TMP/workerd" "$OUT/workerd"
    echo "fetch-workerd: $OUT/workerd (github-release $VERSION, $PLATFORM)"
    ;;

  *)
    echo "unknown workerd.lock source: $SOURCE (expected npm or github-release)" >&2
    exit 1
    ;;
esac
