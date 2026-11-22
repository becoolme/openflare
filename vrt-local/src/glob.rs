//! Wrangler `glob-to-regexp`-compatible matching, shared with the
//! `of-bundler` helper (`#[path]`-included there) so rule globs behave
//! identically on both sides of the subprocess boundary.

#[derive(Debug, Clone, Copy, PartialEq)]
enum GlobTok {
    Lit(char),
    /// `*` — any run of non-`/` chars.
    Star,
    /// `**` between `/`s or string ends — any run incl. `/`, but it may
    /// only stop at a segment boundary (mirrors
    /// `((?:[^/]*(?:/|$))*)`).
    Globstar,
}

/// Mirror of wrangler's `glob-to-regexp` ({globstar:true}, no
/// `extended`): `*`/`**` are the only wildcards — `?`, `[...]`,
/// `{a,b}` are LITERAL characters; `\` escapes the next char into a
/// literal. Whole-string anchored match, memoized so `**` chains
/// cannot backtrack exponentially.
pub(crate) fn glob_match(pattern: &str, path: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let mut toks: Vec<GlobTok> = Vec::with_capacity(p.len());
    let mut i = 0usize;
    while i < p.len() {
        match p[i] {
            '*' => {
                let start = i;
                while p.get(i + 1) == Some(&'*') {
                    i += 1;
                }
                let prev = if start == 0 { None } else { Some(p[start - 1]) };
                let next = p.get(i + 1).copied();
                if i + 1 - start > 1
                    && (prev.is_none() || prev == Some('/'))
                    && (next.is_none() || next == Some('/'))
                {
                    toks.push(GlobTok::Globstar);
                    if next == Some('/') {
                        i += 1;
                    }
                } else {
                    toks.push(GlobTok::Star);
                }
            }
            '\\' => {
                i += 1;
                toks.push(GlobTok::Lit(p.get(i).copied().unwrap_or('\\')));
            }
            c => toks.push(GlobTok::Lit(c)),
        }
        i += 1;
    }
    let s: Vec<char> = path.chars().collect();
    let mut memo: std::collections::HashMap<(usize, usize), bool> =
        std::collections::HashMap::new();
    fn m(
        toks: &[GlobTok],
        ti: usize,
        s: &[char],
        si: usize,
        memo: &mut std::collections::HashMap<(usize, usize), bool>,
    ) -> bool {
        if let Some(&v) = memo.get(&(ti, si)) {
            return v;
        }
        let v = match toks.get(ti) {
            None => si == s.len(),
            Some(GlobTok::Lit(c)) => s.get(si) == Some(c) && m(toks, ti + 1, s, si + 1, memo),
            Some(GlobTok::Star) => {
                let mut k = si;
                let mut ok = false;
                loop {
                    if m(toks, ti + 1, s, k, memo) {
                        ok = true;
                        break;
                    }
                    match s.get(k) {
                        Some(&ch) if ch != '/' => k += 1,
                        _ => break,
                    }
                }
                ok
            }
            Some(GlobTok::Globstar) => {
                let mut ok = m(toks, ti + 1, s, si, memo);
                let mut k = si + 1;
                while !ok && k <= s.len() {
                    if k == s.len() || s[k - 1] == '/' {
                        ok = m(toks, ti + 1, s, k, memo);
                    }
                    k += 1;
                }
                ok
            }
        };
        memo.insert((ti, si), v);
        v
    }
    m(&toks, 0, &s, 0, &mut memo)
}
