//! `.dev.vars` / `--secrets-file` parser — dotenv subset shared by
//! `vrt dev` and `vrt deploy --secrets-file`.
//!
//! Grammar: `KEY=VALUE` lines, `#` comments and blank lines skipped,
//! optional `export ` prefix, value optionally single/double quoted
//! (matching quotes stripped; `\n`, `\t`, `\\`, `\"`, `\'` unescaped
//! inside double quotes). Keys must match `^[A-Za-z_][A-Za-z0-9_]*$`
//! — anything else is rejected, never silently dropped.

use crate::LocalError;
use std::collections::BTreeMap;

/// Parse dotenv text into `KEY -> VALUE`. Invalid lines fail closed
/// with the line number — never the line content (values are secret).
pub fn parse(text: &str) -> Result<BTreeMap<String, String>, LocalError> {
    let mut out = BTreeMap::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
        let Some((key, value)) = line.split_once('=') else {
            return Err(LocalError::Op(
                "devvars",
                format!("line {}: expected KEY=VALUE", i + 1),
            ));
        };
        let key = key.trim();
        let valid = !key.is_empty()
            && key
                .bytes()
                .next()
                .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
            && key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
        if !valid {
            return Err(LocalError::Op(
                "devvars",
                format!("line {}: invalid key", i + 1),
            ));
        }
        let value = unquote(value.trim())
            .map_err(|_| LocalError::Op("devvars", format!("line {}: bad quoting", i + 1)))?;
        out.insert(key.to_string(), value);
    }
    Ok(out)
}

fn unquote(v: &str) -> Result<String, ()> {
    if v.len() >= 2 && v.starts_with('"') && v.ends_with('"') {
        let inner = &v[1..v.len() - 1];
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some('n') => out.push('\n'),
                    Some('t') => out.push('\t'),
                    Some('\\') => out.push('\\'),
                    Some('"') => out.push('"'),
                    Some('\'') => out.push('\''),
                    Some(other) => {
                        out.push('\\');
                        out.push(other);
                    }
                    None => out.push('\\'),
                }
            } else {
                out.push(c);
            }
        }
        Ok(out)
    } else if v.len() >= 2 && v.starts_with('\'') && v.ends_with('\'') {
        Ok(v[1..v.len() - 1].to_string())
    } else {
        Ok(v.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotenv_basics() {
        let m = parse("A=1\n# comment\n\nexport B = two\nC=\"x\\ny\"\nD='lit'\n").unwrap();
        assert_eq!(m["A"], "1");
        assert_eq!(m["B"], "two");
        assert_eq!(m["C"], "x\ny");
        assert_eq!(m["D"], "lit");
    }

    #[test]
    fn rejects_bad_lines() {
        assert!(parse("NO_EQUALS").is_err());
        assert!(parse("1BAD=x").is_err());
        assert!(parse("BAD-KEY=x").is_err());
    }
}
