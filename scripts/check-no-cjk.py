#!/usr/bin/env python3
"""Fail if any tracked file (outside third_party/) contains CJK characters.

Policy: code, comments, test data and config are English/ASCII. Test input that
needs non-ASCII must be written as escapes (Rust "\\u{4e2d}", JS/JSON "\\u4e2d").

Checked ranges: U+3000-U+9FFF and U+FF00-U+FFEF.
"""
import os
import re
import subprocess
import sys

PATTERN = re.compile("[\u3000-\u9fff\uff00-\uffef]")
SKIP_PREFIXES = ("third_party/",)


def repo_root():
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], check=True, stdout=subprocess.PIPE
    ).stdout
    return out.decode("utf-8").strip()


def tracked_files():
    out = subprocess.run(
        ["git", "ls-files", "-z"], check=True, stdout=subprocess.PIPE
    ).stdout
    return [p for p in out.decode("utf-8", "surrogateescape").split("\0") if p]


def main():
    os.chdir(repo_root())
    hits = 0
    for path in tracked_files():
        if path.startswith(SKIP_PREFIXES):
            continue
        try:
            with open(path, "rb") as fh:
                data = fh.read()
        except OSError:
            continue  # deleted in the working tree, or a submodule path
        if b"\0" in data:
            continue  # binary
        text = data.decode("utf-8", "replace")
        for lineno, line in enumerate(text.split("\n"), 1):
            if PATTERN.search(line):
                hits += 1
                print("%s:%d: contains CJK characters" % (path, lineno))
    if hits:
        print(
            "\n%d line(s) contain CJK. Use English/ASCII; write non-ASCII test "
            "input as escapes." % hits,
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
