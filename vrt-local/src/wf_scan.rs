//! W7: static `class_name` checks for `[[workflows]]` declarations —
//! the pack-time half of the deploy gate (control-plane TS mirrors
//! this in `vrtWorkflow.ts::workflowClassProblems`; workerd's
//! `workerd test` preflight is the second line: it rejects missing or
//! non-class exports but happily loads a plain class that is NOT a
//! `WorkflowEntrypoint` — verified empirically on 1.20260916.1 — so
//! the subclass rule must be enforced here).
//!
//! Lexical token scan, NOT a parser — comments and string literals
//! are skipped (no comment-forged `class` can pass). Rules, fail
//! closed on ambiguity:
//! - `export class N extends B` ⇒ N exported, base B.
//! - `class N [extends B]` + `export {N}`/`export {X as N}` ⇒ N is a
//!   class and exported as N.
//! - `export {…} from "…"` ⇒ the listed names are exported (all
//!   module sources are scanned together, mirroring `moduleSources`
//!   TS-side — module-name resolution is not modeled).
//! - `import { WorkflowEntrypoint as A } from "cloudflare:workers"`
//!   ⇒ `A` is a WorkflowEntrypoint root; transitive local chains
//!   (`class B extends A`) resolve by fixpoint.
//! Misjudgment surface (documented): `export *`, `export default`,
//! computed bases (`extends mixin(WE)`), decorators and non-ESM
//! export shapes are unresolvable → REJECTED (deploy fails closed;
//! the operator's fix is a plain `extends WorkflowEntrypoint`).

/// Problem codes a caller maps to its typed error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WfClassProblem {
    /// `class_name` is not a named export of the bundle.
    NotExported(String),
    /// Exported but not a `class` declaration (const/fn/var — the
    /// workerd entrypoint map rejects these too, but the deploy gate
    /// catches them earlier with a precise reason).
    NotAClass(String),
    /// A class export whose `extends` chain never reaches
    /// `WorkflowEntrypoint` (unknown bases fail closed).
    NotEntrypoint(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    /// `'`/`"`/backtick literal — raw body kept (module specifiers).
    Str(String),
    /// `{` `}` `*` `,` `;` `=` etc. — single char.
    Punct(char),
}

fn ident_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$'
}

fn tokenize(src: &str) -> Vec<Tok> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'/' && b.get(i + 1) == Some(&b'/') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
            continue;
        }
        if c == b'\'' || c == b'"' || c == b'`' {
            let q = c;
            let start = i + 1;
            i += 1;
            while i < b.len() && b[i] != q {
                i += if b[i] == b'\\' { 2 } else { 1 };
            }
            out.push(Tok::Str(src[start..i.min(b.len())].to_string()));
            i = (i + 1).min(b.len());
            continue;
        }
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' || c == b'$' {
            let start = i;
            while i < b.len() && ident_char(b[i]) {
                i += 1;
            }
            out.push(Tok::Ident(src[start..i].to_string()));
            continue;
        }
        out.push(Tok::Punct(c as char));
        i += 1;
    }
    out
}

struct Scan {
    /// class name → base (`""` = declared without extends).
    class_bases: std::collections::HashMap<String, String>,
    /// names a workerd entrypoint designator can resolve.
    exported: std::collections::HashSet<String>,
    /// `export { P as Pipe }` → Pipe resolves to local symbol P for
    /// class/subclass checks (exported names can alias declarations).
    export_aliases: std::collections::HashMap<String, String>,
    /// idents that ARE WorkflowEntrypoint (name + import aliases).
    we_roots: std::collections::HashSet<String>,
}

fn scan(sources: &str) -> Scan {
    let toks = tokenize(sources);
    let mut sc = Scan {
        class_bases: std::collections::HashMap::new(),
        exported: std::collections::HashSet::new(),
        export_aliases: std::collections::HashMap::new(),
        we_roots: std::collections::HashSet::from(["WorkflowEntrypoint".to_string()]),
    };
    fn ident(t: &Tok) -> Option<&str> {
        if let Tok::Ident(s) = t {
            Some(s.as_str())
        } else {
            None
        }
    }
    let mut i = 0;
    while i < toks.len() {
        // `class N [extends B]`
        if ident(&toks[i]) == Some("class") {
            if let (Some(n), _) = (toks.get(i + 1).and_then(ident), ()) {
                let n = n.to_string();
                let base = if toks.get(i + 2).and_then(ident) == Some("extends") {
                    toks.get(i + 3).and_then(ident).unwrap_or("").to_string()
                } else {
                    String::new()
                };
                sc.class_bases.insert(n, base);
            }
        }
        // `export …`
        if ident(&toks[i]) == Some("export") {
            match toks.get(i + 1) {
                Some(t) if ident(t) == Some("class") => {
                    if let Some(n) = toks.get(i + 2).and_then(ident) {
                        sc.exported.insert(n.to_string());
                    }
                }
                Some(t)
                    if ident(t) == Some("function")
                        || ident(t) == Some("const")
                        || ident(t) == Some("let")
                        || ident(t) == Some("var") =>
                {
                    if let Some(n) = toks.get(i + 2).and_then(ident) {
                        sc.exported.insert(n.to_string());
                    }
                }
                Some(t) if ident(t) == Some("async") => {
                    // `export async function N`
                    if toks.get(i + 2).and_then(ident) == Some("function") {
                        if let Some(n) = toks.get(i + 3).and_then(ident) {
                            sc.exported.insert(n.to_string());
                        }
                    }
                }
                Some(Tok::Punct('{')) => {
                    // `export {X, Y as Z} [from "…"]` — export names:
                    // `X as Z` exports Z, plain `X` exports X.
                    let mut j = i + 2;
                    while j < toks.len() && toks[j] != Tok::Punct('}') {
                        let mut name = None;
                        if let Tok::Ident(id) = &toks[j] {
                            name = Some(id.clone());
                        }
                        if toks.get(j + 1).and_then(ident) == Some("as") {
                            let local = name.clone();
                            name = toks.get(j + 2).and_then(ident).map(|s| s.to_string());
                            if let (Some(local), Some(exported)) = (local, name.clone()) {
                                if exported != local {
                                    sc.export_aliases.insert(exported, local);
                                }
                            }
                            j += 2;
                        }
                        if let Some(n) = name {
                            sc.exported.insert(n);
                        }
                        j += 1;
                    }
                }
                _ => {}
            }
        }
        // `import {…} from "cloudflare:workers"` — WE aliases. A
        // `from <str>` must follow the brace (else it's a decl stmt).
        if ident(&toks[i]) == Some("import") && toks.get(i + 1) == Some(&Tok::Punct('{')) {
            let mut j = i + 2;
            let mut names = Vec::new();
            while j < toks.len() && toks[j] != Tok::Punct('}') {
                if let Tok::Ident(id) = &toks[j] {
                    names.push(id.clone());
                }
                j += 1;
            }
            let is_we = toks.get(j + 1).and_then(ident) == Some("from")
                && matches!(toks.get(j + 2), Some(Tok::Str(m)) if m == "cloudflare:workers");
            if is_we {
                // `WorkflowEntrypoint` or `WorkflowEntrypoint as A`.
                let mut k = 0;
                while k < names.len() {
                    if names[k] == "WorkflowEntrypoint" {
                        let local = if names.get(k + 1).map(|s| s.as_str()) == Some("as") {
                            names.get(k + 2).cloned().unwrap_or_default()
                        } else {
                            names[k].clone()
                        };
                        sc.we_roots.insert(local);
                    }
                    k += 1;
                }
            }
        }
        i += 1;
    }
    sc
}

/// Scan concatenated module sources; return one problem per failing
/// `class_name` (declaration order). Valid classes contribute nothing.
pub fn workflow_class_problems(sources: &str, class_names: &[String]) -> Vec<WfClassProblem> {
    let sc = scan(sources);
    class_names
        .iter()
        .filter_map(|name| {
            if !sc.exported.contains(name.as_str()) {
                return Some(WfClassProblem::NotExported(name.clone()));
            }
            let local = sc
                .export_aliases
                .get(name.as_str())
                .map(String::as_str)
                .unwrap_or(name);
            if !sc.class_bases.contains_key(local) {
                return Some(WfClassProblem::NotAClass(name.clone()));
            }
            if !is_we_subclass(local, &sc) {
                return Some(WfClassProblem::NotEntrypoint(name.clone()));
            }
            None
        })
        .collect()
}

/// Chain-walk `class_bases` from `name` — true iff some ancestor is a
/// `WorkflowEntrypoint` alias. Cycles/unknown bases terminate false.
fn is_we_subclass(name: &str, sc: &Scan) -> bool {
    let mut cur = name.to_string();
    for _ in 0..sc.class_bases.len() + 1 {
        let Some(base) = sc.class_bases.get(cur.as_str()) else {
            return false;
        };
        if sc.we_roots.contains(base.as_str()) {
            return true;
        }
        if base.is_empty() || *base == cur {
            return false;
        }
        cur = base.clone();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn problems(src: &str, names: &[&str]) -> Vec<WfClassProblem> {
        workflow_class_problems(
            src,
            &names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
    }

    #[test]
    fn missing_export_is_rejected() {
        let src = "export class Flow extends WorkflowEntrypoint {}";
        assert_eq!(
            problems(src, &["Ghost"]),
            vec![WfClassProblem::NotExported("Ghost".into())]
        );
    }

    #[test]
    fn non_class_export_is_rejected() {
        for src in [
            "export const Flow = () => {}",
            "export function Flow() {}",
            "export let Flow = 1",
            "export async function Flow() {}",
        ] {
            assert_eq!(
                problems(src, &["Flow"]),
                vec![WfClassProblem::NotAClass("Flow".into())],
                "{src}"
            );
        }
    }

    #[test]
    fn plain_class_not_entrypoint_is_rejected() {
        for src in [
            "export class Flow {}",
            "export class Flow extends Component {}",
            "export class Flow extends mixin(WorkflowEntrypoint) {}",
            "class Flow extends A {}; class A {}; export { Flow }",
        ] {
            assert_eq!(
                problems(src, &["Flow"]),
                vec![WfClassProblem::NotEntrypoint("Flow".into())],
                "{src}"
            );
        }
    }

    #[test]
    fn entrypoint_subclass_forms_pass() {
        for src in [
            "export class Flow extends WorkflowEntrypoint {}",
            "class Flow extends WorkflowEntrypoint {}; export { Flow }",
            "class Flow extends Base {}; class Base extends WorkflowEntrypoint {}; export { Flow }",
            "import { WorkflowEntrypoint as WE } from \"cloudflare:workers\"; export class Flow extends WE {}",
            "import { WorkflowEntrypoint } from \"cloudflare:workers\"; export class Flow extends WorkflowEntrypoint {}",
            "export { Flow } from \"./flow\"; class Flow extends WorkflowEntrypoint {}",
        ] {
            assert_eq!(problems(src, &["Flow"]), vec![], "{src}");
        }
    }

    #[test]
    fn comment_and_string_forgery_do_not_pass() {
        for src in [
            "// export class Flow extends WorkflowEntrypoint {}\nexport const Flow = 1",
            "/* export class Flow extends WorkflowEntrypoint {} */ export const Flow = 1",
            "export const Flow = \"class Flow extends WorkflowEntrypoint\"",
        ] {
            assert_ne!(problems(src, &["Flow"]), vec![], "{src}");
        }
    }

    #[test]
    fn cyclic_and_self_inheritance_terminate() {
        assert_eq!(
            problems(
                "class A extends B {}; class B extends A {}; export { A }",
                &["A"]
            ),
            vec![WfClassProblem::NotEntrypoint("A".into())]
        );
        assert_eq!(
            problems("class A extends A {}; export { A }", &["A"]),
            vec![WfClassProblem::NotEntrypoint("A".into())]
        );
    }
}
