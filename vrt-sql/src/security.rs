//! SQL capability restrictions (section 3 R5): authorizer whitelist fail closed +
//! sqlite3_limit tightening + progress/interrupt timeout.

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::limits::Limit;
use rusqlite::Connection;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::engine::EngineError;

/// PRAGMA read-only whitelist: name -> (read allowed, write allowed).
/// `foreign_keys` is engine-managed: reads allowed, writes forbidden; journal_mode/synchronous/
/// wal_checkpoint/cache_size etc. are taken over by the engine and users may not change them.
// S3 (T33b): database_list leaks the server's absolute path, compile_options/
// function_list leak build details -- removed. The set of PRAGMAs usable in D1 production is unconfirmed,
// so allow the minimal surface.
const PRAGMA_RO_ALLOW: &[&str] = &[
    "table_info",
    "table_xinfo",
    "index_list",
    "index_info",
    "index_xinfo",
    "integrity_check",
    "quick_check",
    "foreign_key_list",
];

/// FUNCTION allowlist (S2, T33b): enumerated against the actual compiled result of bundled SQLite 3.53.2
/// `pragma_function_list`; unlisted functions (fts*/bm25/
/// rtree*/sqlite_compileoption_*/sqlite_log/load_extension/sqlite_offset
/// and any unknown function) are always rejected. Tests contain a full cross-check assertion.
/// sqlite_source_id is the same class as compileoption (build details) -> rejected;
/// sqlite_version is allowed: version number only, no paths/keys, and clients often use it for compatibility probes.
const FUNCTION_ALLOW: &[&str] = &[
    // core scalars
    "abs",
    "changes",
    "char",
    "coalesce",
    "concat",
    "concat_ws",
    "format",
    "glob",
    "hex",
    "if",
    "ifnull",
    "iif",
    "instr",
    "last_insert_rowid",
    "length",
    "like",
    "likelihood",
    "likely",
    "lower",
    "ltrim",
    "max",
    "min",
    "nullif",
    "octet_length",
    "printf",
    "quote",
    "random",
    "randomblob",
    "replace",
    "round",
    "rtrim",
    "sign",
    "soundex",
    "sqlite_version",
    "substr",
    "substring",
    "subtype",
    "total_changes",
    "trim",
    "typeof",
    "unhex",
    "unicode",
    "unistr",
    "unistr_quote",
    "unlikely",
    "upper",
    "zeroblob",
    "->",
    "->>",
    // aggregates
    "avg",
    "count",
    "group_concat",
    "string_agg",
    "sum",
    "total",
    // window
    "cume_dist",
    "dense_rank",
    "first_value",
    "lag",
    "last_value",
    "lead",
    "nth_value",
    "ntile",
    "percent_rank",
    "rank",
    "row_number",
    // date/time
    "date",
    "time",
    "datetime",
    "julianday",
    "unixepoch",
    "strftime",
    "timediff",
    "current_date",
    "current_time",
    "current_timestamp",
    // JSON1 (scalars + aggregates; the table-valued functions json_each/json_tree do not go through the Function action)
    "json",
    "json_array",
    "json_array_insert",
    "json_array_length",
    "json_error_position",
    "json_extract",
    "json_group_array",
    "json_group_object",
    "json_insert",
    "json_object",
    "json_patch",
    "json_pretty",
    "json_quote",
    "json_remove",
    "json_replace",
    "json_set",
    "json_type",
    "json_valid",
    "jsonb",
    "jsonb_array",
    "jsonb_array_insert",
    "jsonb_extract",
    "jsonb_group_array",
    "jsonb_group_object",
    "jsonb_insert",
    "jsonb_object",
    "jsonb_patch",
    "jsonb_remove",
    "jsonb_replace",
    "jsonb_set",
];

/// Explicit deny list: functions present in the bundled build that **must not** be on the allowlist --
/// tests cross-check it against the full function_list, so a "new function nobody evaluated" cannot be silently allowed.
pub const FUNCTION_KNOWN_DENY: &[&str] = &[
    "load_extension",
    "fts3_tokenizer",
    "fts5",
    "fts5_get_locale",
    "fts5_insttoken",
    "fts5_locale",
    "fts5_source_id",
    "bm25",
    "match",
    "matchinfo",
    "highlight",
    "offsets",
    "optimize",
    "snippet",
    "rank_ok",
    "rtreecheck",
    "rtreedepth",
    "rtreenode",
    "sqlite_compileoption_get",
    "sqlite_compileoption_used",
    "sqlite_log",
    "sqlite_source_id",
    "sqlite_offset",
    "sqlite_dbpage",
];

/// Tables denied for read/write (defense in depth).
/// Note: sqlite_master is not a denied table -- SQLite's internal DDL issues
/// Insert/Update/Read on sqlite_master through the authorizer; denying it would break CREATE TABLE;
/// a user writing sqlite_master directly is rejected by SQLite itself ("may not be modified").
/// `_vrt_dedup` is a ledger invisible to users: reads and writes are both denied, and the engine bypasses via InternalFlag.
const TABLE_WRITE_DENY: &[&str] = &["sqlite_dbpage", "_vrt_dedup"];
const TABLE_READ_DENY: &[&str] = &["sqlite_dbpage", "_vrt_dedup"];

/// Reserved object names: the `_vrt_` (engine ledger/internal tables) and `sqlite_` (internal schema) prefixes.
/// All user DDL on these objects is rejected (S1) -- otherwise a
/// trigger could be attached to `_vrt_dedup` and run arbitrary SQL inside the engine's internal bypass window, evading the authorizer.
/// Note: SQLite itself also rejects user objects with the `sqlite_` prefix; this adds defense in depth.
fn is_vrt_name(name: &str) -> bool {
    let n = name.as_bytes();
    n.len() >= 5 && n[..5].eq_ignore_ascii_case(b"_vrt_")
}

fn is_reserved_object(name: &str) -> bool {
    let n = name.as_bytes();
    (n.len() >= 5 && n[..5].eq_ignore_ascii_case(b"_vrt_"))
        || (n.len() >= 7 && n[..7].eq_ignore_ascii_case(b"sqlite_"))
}

fn deny_table_write(name: &str) -> bool {
    TABLE_WRITE_DENY
        .iter()
        .any(|t| t.eq_ignore_ascii_case(name))
}

fn pragma_allowed(name: &str, value: Option<&str>) -> bool {
    if name.eq_ignore_ascii_case("foreign_keys") {
        return value.is_none(); // reads allowed, writes denied
    }
    // D1 dump() emits `PRAGMA defer_foreign_keys=TRUE;` as its first
    // statement, and executing a dump must restore it. The pragma only
    // defers FK enforcement inside the current transaction; it widens no
    // read/write authority, and its value is restricted to boolean
    // spellings SQLite itself accepts.
    if name.eq_ignore_ascii_case("defer_foreign_keys") {
        return match value {
            None => true,
            Some(v) => matches!(
                v.to_ascii_lowercase().as_str(),
                "0" | "1" | "true" | "false" | "on" | "off"
            ),
        };
    }
    if name.eq_ignore_ascii_case("miniflare_d1_export") {
        return false;
    }
    // The authorizer also passes the parenthesized argument of `PRAGMA x(arg)` as value --
    // everything on the allowlist is a read-only pragma, allowed with or without an argument; write pragmas are not in the table.
    PRAGMA_RO_ALLOW.iter().any(|p| p.eq_ignore_ascii_case(name))
}

pub fn function_allowed(name: &str) -> bool {
    FUNCTION_ALLOW.iter().any(|f| f.eq_ignore_ascii_case(name))
}

pub fn authorize(ctx: AuthContext) -> Authorization {
    match ctx.action {
        // cross-database escape and path injection
        AuthAction::Attach { .. } | AuthAction::Detach { .. } => Authorization::Deny,
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } => {
            if pragma_allowed(pragma_name, pragma_value) {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        AuthAction::Function { function_name } => {
            if function_allowed(function_name) {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        AuthAction::Read { table_name, .. } => {
            // Note: the sqlite_ prefix must not be merged in -- the internal writes of CREATE TABLE
            // issue Read/Insert/Update on sqlite_master, which must be allowed;
            // _vrt_dedup is covered by the explicit denied-table list; T37: the `_vrt_q_*` queue internal tables
            // are likewise rejected (every `_vrt_q_` read/write is unreachable from the user surface).
            if TABLE_READ_DENY
                .iter()
                .any(|t| t.eq_ignore_ascii_case(table_name))
                || table_name.to_ascii_lowercase().starts_with("_vrt_q_")
            {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        AuthAction::Insert { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::Update { table_name, .. } => {
            // Same as above: internal DML on sqlite_master must be allowed (CREATE TABLE depends on it).
            // T37: user writes to the `_vrt_q_*` queue internal tables are likewise rejected.
            if deny_table_write(table_name)
                || table_name.to_ascii_lowercase().starts_with("_vrt_q_")
            {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        // DDL: object names with a reserved prefix are always rejected (S1 trigger/index/view bypass protection).
        // Internal writes to sqlite_master are unaffected (that is DML, not DDL).
        AuthAction::CreateIndex {
            index_name,
            table_name,
        }
        | AuthAction::CreateTempIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropTempIndex {
            index_name,
            table_name,
        }
        | AuthAction::CreateTrigger {
            trigger_name: index_name,
            table_name,
        }
        | AuthAction::CreateTempTrigger {
            trigger_name: index_name,
            table_name,
        }
        | AuthAction::DropTrigger {
            trigger_name: index_name,
            table_name,
        }
        | AuthAction::DropTempTrigger {
            trigger_name: index_name,
            table_name,
        } => {
            // Object names only check _vrt_: sqlite_autoindex_* is a name SQLite generates itself
            // (UNIQUE/PK constraints on user tables produce it too), and users likewise cannot create
            // sqlite_* objects; the table-name check covers triggers/indexes on _vrt_dedup.
            if is_reserved_object(table_name) || is_vrt_name(index_name) {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        // AUTOINCREMENT (D1 supports it): when the first AUTOINCREMENT table is created, SQLite
        // internally nests `CREATE TABLE sqlite_sequence(name,seq)`, and that statement also goes through the
        // authorizer as CreateTable{sqlite_sequence}. Only this one exact name is allowed,
        // and only as CreateTable (not Temp/Drop/Alter); user-written DDL with the same name is rejected by SQLite
        // itself with "object name reserved for internal use" (locked by a test),
        // and other `sqlite_`-prefixed names are still all rejected.
        AuthAction::CreateTable { table_name } if table_name == "sqlite_sequence" => {
            Authorization::Allow
        }
        AuthAction::CreateTable { table_name }
        | AuthAction::CreateTempTable { table_name }
        | AuthAction::DropTable { table_name }
        | AuthAction::DropTempTable { table_name }
        | AuthAction::AlterTable { table_name, .. } => {
            if is_reserved_object(table_name) {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        AuthAction::CreateView { view_name }
        | AuthAction::CreateTempView { view_name }
        | AuthAction::DropView { view_name }
        | AuthAction::DropTempView { view_name } => {
            if is_reserved_object(view_name) {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        AuthAction::Select
        | AuthAction::Reindex { .. }
        | AuthAction::Analyze { .. }
        | AuthAction::Savepoint { .. }
        | AuthAction::Transaction { .. }
        | AuthAction::Recursive => Authorization::Allow,
        // Conservative default: vtab module creation (could introduce a custom vtab) and all other unknown codes are rejected.
        // Note: the authorizer code table of bundled SQLite 3.53.2 ends at RECURSIVE=33,
        // and VACUUM has no dedicated action code -- `VACUUM INTO` is intercepted by a statement-text precheck
        // (engine::precheck_sql); the authorizer still rejects any Unknown as a backstop.
        _ => Authorization::Deny,
    }
}

/// Engine-internal statement bypass: the authorizer only applies to user SQL.
/// Engine-managed statements (PRAGMA settings, _vrt_dedup create/read/write ledger,
/// pragma_page_* size statistics) execute first under the `internal` flag;
/// the flag can only be set by engine code (Mutex serialization guarantees no concurrent leak).
#[derive(Clone, Default)]
pub struct InternalFlag(pub Arc<AtomicBool>);

impl InternalFlag {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
    pub fn set(&self, v: bool) {
        self.0.store(v, Ordering::SeqCst);
    }
}

/// Limit values (section 3; hard upper bounds on the SQLite side, provider precheck is separate).
/// SQL_LENGTH 100KB / VARIABLE_NUMBER 100 (Cloudflare's public limits,
/// the doc marks them as pending online verification -- implemented as such for now).
/// Returns InternalFlag: the authorizer bypass switch for engine-internal statements.
pub fn configure(conn: &Connection) -> Result<InternalFlag, EngineError> {
    conn.set_limit(Limit::SQLITE_LIMIT_ATTACHED, 0)?;
    conn.set_limit(Limit::SQLITE_LIMIT_SQL_LENGTH, 100 * 1024)?;
    conn.set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 100)?;
    conn.set_limit(Limit::SQLITE_LIMIT_EXPR_DEPTH, 1_000)?;
    conn.set_limit(Limit::SQLITE_LIMIT_TRIGGER_DEPTH, 10)?;
    conn.set_limit(Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 50_000)?;
    conn.set_limit(Limit::SQLITE_LIMIT_WORKER_THREADS, 0)?;
    // S2: backstop upper bound for amplifying constructs such as randomblob/zeroblob/printf('%.*c').
    // 32MiB is a conservative figure (Cloudflare's actual row/blob limit is pending online verification).
    conn.set_limit(Limit::SQLITE_LIMIT_LENGTH, 32 * 1024 * 1024)?;
    conn.set_limit(Limit::SQLITE_LIMIT_COLUMN, 2_000)?;
    conn.set_limit(Limit::SQLITE_LIMIT_FUNCTION_ARG, 127)?;
    let internal = InternalFlag::new();
    let flag = internal.clone();
    conn.authorizer(Some(move |ctx: AuthContext<'_>| {
        if flag.0.load(Ordering::SeqCst) {
            // S1 defense in depth: even inside the internal window only the actions the engine expects are allowed --
            // any unexpected action (e.g. a malicious trigger body pulled into
            // prepare while the engine writes its ledger) is still rejected; better to wrongly reject the engine itself (fail closed).
            authorize_internal(ctx)
        } else {
            authorize(ctx)
        }
    }))?;
    Ok(internal)
}

/// Engine internal tables: the `_vrt_dedup` ledger + `_vrt_q_*` queue objects (T37).
fn is_internal_table(name: &str) -> bool {
    name.eq_ignore_ascii_case("_vrt_dedup") || name.to_ascii_lowercase().starts_with("_vrt_q_")
}

/// Full set of expected actions for internal statements: init PRAGMA, _vrt_dedup table creation,
/// SELECT/INSERT on _vrt_dedup, pragma_page_* size statistics. Everything else is rejected.
fn authorize_internal(ctx: AuthContext) -> Authorization {
    match ctx.action {
        AuthAction::Pragma { pragma_name, .. } => {
            // Table-valued pragma functions (pragma_page_count() etc.) were observed to emit a
            // Pragma{name} action rather than Function -- size_after depends on them.
            // T36: `wal_checkpoint` is an internal action of the engine persistence layer (WAL frame watermark
            // read + TRUNCATE checkpoint) -- allowed only inside the internal window;
            // the user pragma path still goes through the allowlist (which does not contain it).
            if matches!(
                pragma_name.to_ascii_lowercase().as_str(),
                "journal_mode"
                    | "synchronous"
                    | "page_count"
                    | "page_size"
                    | "wal_checkpoint"
                    | "wal_autocheckpoint"
            ) {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        AuthAction::CreateTable { table_name } | AuthAction::CreateIndex { table_name, .. }
            if is_internal_table(table_name) =>
        {
            // The PK auto-index sqlite_autoindex__vrt_dedup_N also goes through here.
            // T37: the `_vrt_q_*` queue tables (messages/dead/meta + the _vrt_q_visible
            // index) are also engine-internal objects, and the user surface still rejects them via the reserved `_vrt_` prefix.
            Authorization::Allow
        }
        AuthAction::Select | AuthAction::Transaction { .. } | AuthAction::Recursive => {
            Authorization::Allow
        }
        // T37: when CREATE INDEX creates an index it emits a further Reindex
        // action on the index itself (observed on 3.53.2) -- allow the internal index names, reject others.
        AuthAction::Reindex { index_name } => {
            if is_internal_table(index_name) {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        // DML write-back to sqlite_master by internal DDL + _vrt_dedup reads/writes
        AuthAction::Read { table_name, .. }
        | AuthAction::Insert { table_name }
        | AuthAction::Update { table_name, .. }
        | AuthAction::Delete { table_name } => {
            if is_internal_table(table_name)
                || table_name.eq_ignore_ascii_case("sqlite_master")
                || table_name.to_ascii_lowercase().starts_with("pragma_")
            {
                // Read of the table-valued functions pragma_page_count/pragma_page_size
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        AuthAction::Function { function_name } => {
            if function_allowed(function_name) {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        _ => Authorization::Deny,
    }
}

/// `VACUUM` statement-text precheck: strip line comments + collapse whitespace, then match in lowercase.
/// T33 decided to [ban VACUUM entirely] (deviating from section 3, which allowed bare VACUUM): bundled 3.53.2's
/// authorizer has no VACUUM action code; a bare VACUUM was observed to go internally through
/// `Attach{filename:""}`, indistinguishable at the authorizer layer from the user's `ATTACH ''`,
/// and ATTACH must be rejected wholesale -- so both VACUUM forms are banned, to be refined once replication lands.
pub fn looks_like_vacuum(sql: &str) -> bool {
    let normalized: String = sql
        .lines()
        .map(|l| {
            let t = l.trim_start();
            if t.starts_with("--") {
                ""
            } else {
                t
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    normalized.contains("vacuum")
}

/// Deadline-aware progress handler: checks every ~10k VDBE ops,
/// returning true on expiry (interrupting the current statement).
pub fn arm_deadline(conn: &Connection, deadline: std::time::Instant) {
    conn.progress_handler(10_000, Some(move || std::time::Instant::now() >= deadline))
        .expect("progress_handler install");
}

pub fn disarm_deadline(conn: &Connection) {
    conn.progress_handler(0, None::<fn() -> bool>)
        .expect("progress_handler clear");
}
