//! T37r P0: the emitted `queue` binding must be a `ServiceDesignator` —
//! workerd.capnp is the authoritative schema. This test parses the
//! vendored schema text and proves (a) `queue @15 :ServiceDesignator`
//! exists in `Worker.Binding`, and (b) every field name our renderer
//! puts inside `queue = (...)` belongs to `ServiceDesignator`'s fields
//! (name/entrypoint/props; props-union: empty/json). Field names are
//! never invented from memory again.

mod common;

use common::*;
use vrt_config::render_tenant_config;

/// Locate `workerd.capnp`: `$WORKERD_CAPNP`, else `node_modules/workerd`
/// (npm layout) or `node_modules/.pnpm/workerd@*` under the repo root.
/// Returns `None` (test skips) when workerd is not installed; run
/// `npm install` at the repo root to enable these schema checks.
fn workerd_capnp() -> Option<String> {
    if let Ok(p) = std::env::var("WORKERD_CAPNP") {
        return Some(std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("cannot read {p}: {e}")));
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let npm = root.join("node_modules/workerd/workerd.capnp");
    if npm.exists() {
        return Some(std::fs::read_to_string(npm).expect("workerd.capnp readable"));
    }
    let mut candidates: Vec<std::path::PathBuf> = std::fs::read_dir(root.join("node_modules/.pnpm"))
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let dir = e.ok()?.path();
            let name = dir.file_name()?.to_str()?.to_owned();
            name.starts_with("workerd@")
                .then(|| dir.join("node_modules/workerd/workerd.capnp"))
        })
        .filter(|p| p.exists())
        .collect();
    candidates.sort();
    match candidates.last() {
        Some(p) => Some(std::fs::read_to_string(p).expect("workerd.capnp readable")),
        None => {
            eprintln!("skip: workerd.capnp not found (set WORKERD_CAPNP or npm install)");
            None
        }
    }
}

/// Parse `struct <name> { ... }` and collect its field names: the
/// `fieldName @N :Type` and `unionMember @N :Type` declarations.
fn struct_fields(schema: &str, struct_name: &str) -> Vec<String> {
    let marker = format!("struct {struct_name} {{");
    let start = schema
        .find(&marker)
        .unwrap_or_else(|| panic!("struct {struct_name} not found in workerd.capnp"))
        + marker.len();
    // Struct body ends at the matching close brace at column 0.
    let rest = &schema[start..];
    let end = rest.find("\n}").expect("struct has closing brace");
    rest[..end]
        .lines()
        .filter_map(|l| {
            let l = l.trim();
            if let Some((name, _)) = l.split_once('@') {
                return Some(name.trim().to_owned());
            }
            // Named union groups (`props :union {`) carry no ordinal —
            // the group name itself is the field name.
            if let Some((name, _)) = l.split_once(":union") {
                return Some(name.trim().to_owned());
            }
            if let Some((name, _)) = l.split_once(":group") {
                return Some(name.trim().to_owned());
            }
            None
        })
        .collect()
}

/// Extract the field names used inside one emitted struct literal:
/// `foo = (a = 1, b = 2, c = (d = 3))` → ["a","b","c"] (top level) and
/// nested `(d = ...)` content is re-parsed by the caller when needed.
fn emitted_fields(fragment: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut cur = String::new();
    for ch in fragment.chars() {
        match ch {
            '(' => {
                depth += 1;
                cur.clear();
            }
            ')' => {
                depth = depth.saturating_sub(1);
                cur.clear();
            }
            '=' if depth == 1 => {
                let frag = cur.rsplit([',', '\n']).next().unwrap_or("").trim();
                if !frag.is_empty() && frag.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    out.push(frag.to_owned());
                }
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    out
}

/// Grab the balanced `( ... )` group following `key = (` inside `text`.
fn balanced_group<'a>(text: &'a str, key: &str) -> &'a str {
    let needle = format!("{key} = (");
    let start = text
        .find(&needle)
        .unwrap_or_else(|| panic!("{needle} not emitted"))
        + needle.len()
        - 1;
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    for (i, &b) in bytes[start..].iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return &text[start..start + i + 1];
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced group for {key}")
}

#[test]
fn queue_binding_is_service_designator() {
    let Some(schema) = workerd_capnp() else { return };

    // (a) `queue` is a ServiceDesignator-typed union member of Binding.
    let binding = struct_fields(&schema, "Binding");
    // workerd.capnp names the struct `Worker.Binding` — the emitted
    // `struct Worker { bindings :union { ... } }` may nest; accept either.
    assert!(
        binding.iter().any(|f| f == "queue") || schema.contains("queue @15 :ServiceDesignator"),
        "workerd.capnp must declare `queue @15 :ServiceDesignator`"
    );
    assert!(
        schema.contains("queue @15 :ServiceDesignator"),
        "`queue` binding must be :ServiceDesignator (schema line 406)"
    );

    // (b) ServiceDesignator's legal field names.
    let designator = struct_fields(&schema, "ServiceDesignator");
    for required in ["name", "entrypoint", "props", "empty", "json"] {
        assert!(
            designator.iter().any(|f| f == required),
            "ServiceDesignator must expose `{required}`: got {designator:?}"
        );
    }

    // (c) Render and extract the emitted queue designator.
    let tenant = tenant();
    let bundles = vec![bundle_main(), bundle_aux()];
    let params = runtime_params();
    let rendered = render_tenant_config(&input(&tenant, &bundles, &params)).unwrap();
    assert!(
        rendered.text.contains("\"JOBS\""),
        "fixture emits JOBS queue binding"
    );
    let group = balanced_group(&rendered.text, "queue");
    let fields = emitted_fields(group);
    assert_eq!(
        fields,
        vec![
            "name".to_string(),
            "entrypoint".to_string(),
            "props".to_string()
        ],
        "queue designator must emit exactly name+entrypoint+props, got {fields:?}"
    );
    for f in &fields {
        assert!(
            designator.iter().any(|d| d == f),
            "emitted queue field `{f}` is not a ServiceDesignator field"
        );
    }
    // The props union member we emit is `json` — legal union member.
    let props_group = balanced_group(group, "props");
    let props_fields = emitted_fields(props_group);
    assert_eq!(props_fields, vec!["json".to_string()]);
    assert!(designator.iter().any(|d| d == "json"));
}

/// Every binding type vrt-config emits must exist in `Worker.Binding`'s
/// union — catches a second class of invented-field bugs.
#[test]
fn all_emitted_binding_kinds_exist_in_schema() {
    let Some(schema) = workerd_capnp() else { return };
    let tenant = tenant();
    let bundles = vec![bundle_main(), bundle_aux()];
    let params = runtime_params();
    let rendered = render_tenant_config(&input(&tenant, &bundles, &params)).unwrap();
    // Emitted `(name = "X", kind = (...))` — collect the union member each
    // binding uses by scanning `name = "…", <kind> = ` at depth 1.
    let mut kinds = std::collections::BTreeSet::new();
    for line in rendered.text.lines() {
        let l = line.trim();
        if !(l.starts_with("(name = ") && l.contains(" = (")) {
            continue;
        }
        // second ` = (` after the first name field opens the kind group.
        if let Some(rest) = l.strip_prefix("(name = ") {
            if let Some(pos) = rest.find("\", ") {
                let kind = rest[pos + 3..].split(" = ").next().unwrap_or("").trim();
                if !kind.is_empty() && kind.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    kinds.insert(kind.to_owned());
                }
            }
        }
    }
    for k in &kinds {
        // Named union members appear either as `kind @N` (ordinal
        // members) or `kind :group`/`kind :union` (grouped members like
        // `workerLoader`, which carry no ordinal of their own).
        assert!(
            schema.contains(&format!("{k} @"))
                || schema.contains(&format!("{k} :group"))
                || schema.contains(&format!("{k} :union")),
            "binding union member `{k}` missing from workerd.capnp"
        );
    }
    assert!(kinds.contains("queue"), "queue binding must be emitted");
}

/// Worker log collection relies on `Worker.tails` being a list of
/// ServiceDesignator (tail handler delivery of TraceItems).
#[test]
fn worker_tails_is_service_designator_list() {
    let Some(schema) = workerd_capnp() else { return };
    assert!(
        schema.contains("tails @14 :List(ServiceDesignator)"),
        "workerd.capnp must declare `tails @14 :List(ServiceDesignator)`"
    );
    assert!(
        schema.contains("streamingTails @15 :List(ServiceDesignator)"),
        "workerd.capnp must declare `streamingTails @15 :List(ServiceDesignator)`"
    );
}
