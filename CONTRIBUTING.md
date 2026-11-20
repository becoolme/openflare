# Contributing

Code, comments, test data and config must be English/ASCII (CI runs `scripts/check-no-cjk.py`); write non-ASCII test input as escapes (`"\u{4e2d}"` in Rust, `"\u{4e2d}"` in JS/JSON).

Every `unsafe` block in production code (`--lib --bins`) must have a
`// SAFETY:` comment on the preceding line stating why the block's
preconditions hold. CI enforces it with
`cargo clippy --workspace --lib --bins -- -D clippy::undocumented_unsafe_blocks`;
run the same command locally before pushing.
