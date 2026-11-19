//! SQL 能力限制（§3 R5）：authorizer 白名单 fail closed +
//! sqlite3_limit 收口 + progress/interrupt 超时。

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::limits::Limit;
use rusqlite::Connection;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::engine::EngineError;

/// PRAGMA 只读白名单：name → (允许读, 允许写)。
/// `foreign_keys` 引擎托管：读放行写禁止；journal_mode/synchronous/
/// wal_checkpoint/cache_size 等由引擎统一接管，禁用户改。
// S3（T33b）：database_list 泄漏服务器绝对路径，compile_options/
// function_list 泄漏构建细节——移除。D1 生产可用 PRAGMA 清单未证实，
// 按最小面放行。
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

/// FUNCTION 白名单（S2，T33b）：对照 bundled SQLite 3.53.2
/// `pragma_function_list` 实际编译结果枚举；未列出的函数（fts*/bm25/
/// rtree*/sqlite_compileoption_*/sqlite_log/load_extension/sqlite_offset
/// 及任何未知函数）一律拒。tests 里有全量对照断言。
/// sqlite_source_id 与 compileoption 同类（构建细节）→ 拒；
/// sqlite_version 放行：仅版本号、不含路径/密钥，客户端常做兼容探测。
const FUNCTION_ALLOW: &[&str] = &[
    // 核心标量
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
    // 聚合
    "avg",
    "count",
    "group_concat",
    "string_agg",
    "sum",
    "total",
    // 窗口
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
    // 日期时间
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
    // JSON1（标量+聚合；表值函数 json_each/json_tree 不走 Function action）
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

/// 显式拒绝名单：bundled 中存在但**不应**出现在白名单的函数——
/// 测试用它与 function_list 全量对照，防止"忘了评估的新函数"静默放行。
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

/// Read/写禁表（防御纵深）。
/// 注意：sqlite_master 不在禁表——SQLite 内部 DDL 会经 authorizer 产生
/// 对 sqlite_master 的 Insert/Update/Read，拒之会连带破坏 CREATE TABLE；
/// 用户直接写 sqlite_master 由 SQLite 自身拒（"may not be modified"）。
/// `_vrt_dedup` 为用户不可见账本：读写都拒，引擎走 InternalFlag 旁路。
const TABLE_WRITE_DENY: &[&str] = &["sqlite_dbpage", "_vrt_dedup"];
const TABLE_READ_DENY: &[&str] = &["sqlite_dbpage", "_vrt_dedup"];

/// 保留对象名：`_vrt_`（引擎账本/内部表）与 `sqlite_`（内部 schema）前缀。
/// 用户对这些对象的一切 DDL 全拒（S1）——否则可在 `_vrt_dedup` 上挂
/// trigger，在引擎 internal 旁路窗口内执行任意 SQL 绕过 authorizer。
/// 注：`sqlite_` 前缀用户对象 SQLite 自身也会拒，此处叠防御纵深。
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
        return value.is_none(); // 读放行写禁
    }
    if name.eq_ignore_ascii_case("miniflare_d1_export") {
        return false;
    }
    // authorizer 把 `PRAGMA x(arg)` 的括号参数也作为 value 传入——
    // 白名单内均为只读 pragma，带参与否都放行；写型 pragma 不在表内。
    PRAGMA_RO_ALLOW.iter().any(|p| p.eq_ignore_ascii_case(name))
}

pub fn function_allowed(name: &str) -> bool {
    FUNCTION_ALLOW.iter().any(|f| f.eq_ignore_ascii_case(name))
}

pub fn authorize(ctx: AuthContext) -> Authorization {
    match ctx.action {
        // 跨库逃逸与路径注入
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
            // 注意：不能把 sqlite_ 前缀并入——CREATE TABLE 的内部写会
            // 对 sqlite_master 发 Read/Insert/Update，须放行；
            // _vrt_dedup 由显式禁表覆盖；T37：`_vrt_q_*` 队列内部表
            // 同拒（用户面一切 `_vrt_q_` 读写都不可达）。
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
            // 同上：sqlite_master 的内部 DML 必须放行（CREATE TABLE 依赖）。
            // T37：`_vrt_q_*` 队列内部表用户写同拒。
            if deny_table_write(table_name)
                || table_name.to_ascii_lowercase().starts_with("_vrt_q_")
            {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        // DDL：对象名带保留前缀一律拒（S1 trigger/索引/视图绕过防护）。
        // sqlite_master 的内部写不受此影响（那是 DML 不是 DDL）。
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
            // 对象名只查 _vrt_：sqlite_autoindex_* 是 SQLite 自生成名
            // （UNIQUE/PK 约束在用户表上也会产生），用户同样建不出
            // sqlite_* 对象；表名检查兜住 _vrt_dedup 上的 trigger/index。
            if is_reserved_object(table_name) || is_vrt_name(index_name) {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        // AUTOINCREMENT（D1 支持）：SQLite 在首个 AUTOINCREMENT 表建表时
        // 内部嵌套 `CREATE TABLE sqlite_sequence(name,seq)`，该语句同样经
        // authorizer 发 CreateTable{sqlite_sequence}。只放行这一个精确名字、
        // 且仅 CreateTable（非 Temp/Drop/Alter）；用户手写同名 DDL 由 SQLite
        // 自身以 "object name reserved for internal use" 拒绝（有测试锁定），
        // `sqlite_` 其它前缀名仍全拒。
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
        // 保守默认：vtab 模块创建（可引入自定义 vtab）与其余未知 code 拒。
        // 注：bundled SQLite 3.53.2 的 authorizer 码表止于 RECURSIVE=33，
        // VACUUM 无专用 action code——`VACUUM INTO` 由语句文本预检拦截
        // （engine::precheck_sql），authorizer 仍把任何 Unknown 拒死兜底。
        _ => Authorization::Deny,
    }
}

/// 引擎内部语句旁路：authorizer 只对用户 SQL 生效。
/// 引擎管理语句（PRAGMA 设置、_vrt_dedup 建表/读写账、
/// pragma_page_* 尺寸统计）先在 `internal` 标志下执行；
/// 该标志只能由引擎代码置位（Mutex 串行化保证无并发泄漏）。
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

/// 限额数值（§3；SQLite 侧硬上界，provider 预检另算）。
/// SQL_LENGTH 100KB / VARIABLE_NUMBER 100（Cloudflare 公开限额口径，
/// 文档标注待联网核实——先按此实现）。
/// 返回 InternalFlag：引擎内部语句的 authorizer 旁路开关。
pub fn configure(conn: &Connection) -> Result<InternalFlag, EngineError> {
    conn.set_limit(Limit::SQLITE_LIMIT_ATTACHED, 0)?;
    conn.set_limit(Limit::SQLITE_LIMIT_SQL_LENGTH, 100 * 1024)?;
    conn.set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 100)?;
    conn.set_limit(Limit::SQLITE_LIMIT_EXPR_DEPTH, 1_000)?;
    conn.set_limit(Limit::SQLITE_LIMIT_TRIGGER_DEPTH, 10)?;
    conn.set_limit(Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 50_000)?;
    conn.set_limit(Limit::SQLITE_LIMIT_WORKER_THREADS, 0)?;
    // S2：randomblob/zeroblob/printf('%.*c') 等放大构造的兜底上界。
    // 32MiB 为保守口径（Cloudflare 实际行/blob 上限待联网核实）。
    conn.set_limit(Limit::SQLITE_LIMIT_LENGTH, 32 * 1024 * 1024)?;
    conn.set_limit(Limit::SQLITE_LIMIT_COLUMN, 2_000)?;
    conn.set_limit(Limit::SQLITE_LIMIT_FUNCTION_ARG, 127)?;
    let internal = InternalFlag::new();
    let flag = internal.clone();
    conn.authorizer(Some(move |ctx: AuthContext<'_>| {
        if flag.0.load(Ordering::SeqCst) {
            // S1 纵深防御：internal 窗口内也只放行引擎预期动作——
            // 任何非预期 action（例如恶意 trigger 体在引擎写账时被
            // 带入 prepare）仍被拒，宁可误杀引擎自身（fail closed）。
            authorize_internal(ctx)
        } else {
            authorize(ctx)
        }
    }))?;
    Ok(internal)
}

/// 引擎内部表：`_vrt_dedup` 账本 + `_vrt_q_*` 队列对象（T37）。
fn is_internal_table(name: &str) -> bool {
    name.eq_ignore_ascii_case("_vrt_dedup") || name.to_ascii_lowercase().starts_with("_vrt_q_")
}

/// internal 语句的预期 action 全集：init PRAGMA、_vrt_dedup 建表、
/// SELECT/INSERT _vrt_dedup、pragma_page_* 尺寸统计。其余一概拒。
fn authorize_internal(ctx: AuthContext) -> Authorization {
    match ctx.action {
        AuthAction::Pragma { pragma_name, .. } => {
            // 表值 pragma 函数（pragma_page_count() 等）实测发
            // Pragma{name} 动作而非 Function——size_after 依赖它们。
            // T36：`wal_checkpoint` 是引擎持久层内部 action（WAL 帧水位
            // 读取 + TRUNCATE checkpoint）——只在 internal 窗口放行，
            // 用户 pragma 路径仍走白名单（白名单不含它）。
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
            // PK 自动索引 sqlite_autoindex__vrt_dedup_N 也走这里。
            // T37：`_vrt_q_*` 队列表（messages/dead/meta + _vrt_q_visible
            // 索引）同属引擎内部对象，用户面仍被 `_vrt_` 保留前缀全拒。
            Authorization::Allow
        }
        AuthAction::Select | AuthAction::Transaction { .. } | AuthAction::Recursive => {
            Authorization::Allow
        }
        // T37：CREATE INDEX 在建索引时对索引本身再发 Reindex
        // action（实测 3.53.2）——放行内部索引名，拒其它。
        AuthAction::Reindex { index_name } => {
            if is_internal_table(index_name) {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        // 内部 DDL 对 sqlite_master 的 DML 回写 + _vrt_dedup 读写
        AuthAction::Read { table_name, .. }
        | AuthAction::Insert { table_name }
        | AuthAction::Update { table_name, .. }
        | AuthAction::Delete { table_name } => {
            if is_internal_table(table_name)
                || table_name.eq_ignore_ascii_case("sqlite_master")
                || table_name.to_ascii_lowercase().starts_with("pragma_")
            {
                // pragma_page_count/pragma_page_size 表值函数的 Read
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

/// `VACUUM` 语句文本预检：剥行注释+折叠空白后小写匹配。
/// T33 决定【全禁 VACUUM】（偏离 §3 的裸 VACUUM 放行）：bundled 3.53.2
/// authorizer 无 VACUUM action code；实测裸 VACUUM 内部走
/// `Attach{filename:""}`，与用户 `ATTACH ''` 在 authorizer 层不可区分，
/// 而 ATTACH 必须整体拒——故 VACUUM 两种形态都禁，待复制落地再细化。
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

/// deadline 感知的 progress handler：每 ~10k VDBE ops 检查一次，
/// 到期返回 true（中断当前语句）。
pub fn arm_deadline(conn: &Connection, deadline: std::time::Instant) {
    conn.progress_handler(10_000, Some(move || std::time::Instant::now() >= deadline))
        .expect("progress_handler install");
}

pub fn disarm_deadline(conn: &Connection) {
    conn.progress_handler(0, None::<fn() -> bool>)
        .expect("progress_handler clear");
}
