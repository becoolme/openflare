//! Typed errors for `vrt-config`. Every variant carries a stable
//! `BUNDLE_*` / `CONFIG_*` protocol code and never embeds attacker
//! controlled input in its message (only `'static` field identifiers).

/// Errors from manifest validation, bundle construction, and stored-bundle
/// verification.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BundleError {
    /// The input document is not a well-formed manifest (typed decode or
    /// pack-manifest shape failed); payload names the field.
    #[error("malformed manifest: {0}")]
    Manifest(&'static str),

    /// A module name is unsafe: empty, over-length, absolute, contains
    /// `..`, backslash, NUL/control characters, or `:`.
    #[error("invalid module name")]
    ModuleName,

    /// The declared main module is not present in `modules`.
    #[error("main module is not in the module list")]
    MainModule,

    /// A module name appears twice.
    #[error("duplicate module name")]
    DuplicateModule,

    /// A size/count bound was exceeded; payload names the bound
    /// (`modules`, `moduleBytes`, `totalBytes`, `compatibilityFlags`,
    /// `nameBytes`, `storedBytes`).
    #[error("capacity bound exceeded: {0}")]
    Capacity(&'static str),

    /// `compatibilityDate` is not strict `YYYY-MM-DD`.
    #[error("invalid compatibility date")]
    CompatibilityDate,

    /// A compatibility-flag token is malformed.
    #[error("invalid compatibility flag")]
    CompatibilityFlag,

    /// Module content cannot be represented for its declared type
    /// (bad base64, non-UTF-8 text, or unparseable JSON).
    #[error("invalid module content")]
    ModuleContent,

    /// An unknown module wire form or module type was supplied.
    #[error("unsupported module type")]
    ModuleType,

    /// A digest field is not 64 lowercase hex chars.
    #[error("malformed digest")]
    Digest,

    /// A stored bundle document fails verification: recomputed digest
    /// differs from the declared one, or a module digest mismatches.
    #[error("stored bundle does not verify")]
    StoredVerify,
}

impl BundleError {
    /// Stable protocol error code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Manifest(_) => "BUNDLE_INVALID_MANIFEST",
            Self::ModuleName => "BUNDLE_INVALID_MODULE_NAME",
            Self::MainModule => "BUNDLE_MAIN_MODULE_MISSING",
            Self::DuplicateModule => "BUNDLE_DUPLICATE_MODULE",
            Self::Capacity(_) => "BUNDLE_CAPACITY_EXCEEDED",
            Self::CompatibilityDate => "BUNDLE_INVALID_COMPATIBILITY_DATE",
            Self::CompatibilityFlag => "BUNDLE_INVALID_COMPATIBILITY_FLAG",
            Self::ModuleContent => "BUNDLE_INVALID_MODULE_CONTENT",
            Self::ModuleType => "BUNDLE_UNSUPPORTED_MODULE_TYPE",
            Self::Digest => "BUNDLE_INVALID_DIGEST",
            Self::StoredVerify => "BUNDLE_STORED_VERIFY_FAILED",
        }
    }
}

/// Errors from workerd capnp generation for one tenant process.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// A structural identifier is outside its whitelist (service names,
    /// entrypoints, socket dir, worker ids); payload is the field name.
    #[error("invalid identifier: {0}")]
    Identifier(&'static str),

    /// A declared `var`/`secret` binding has no resolved value in the run
    /// parameters; payload names the reference kind (`varRef`/`secretRef`).
    #[error("unresolved reference: {0}")]
    UnresolvedReference(&'static str),

    /// A service binding targets a worker id outside this tenant's worker
    /// set (defense in depth behind `compute.desired` validation).
    #[error("service binding target not in tenant workers")]
    ServiceTarget,

    /// A worker's `artifactDigest` has no matching [`crate::VersionBundle`]
    /// (missing, extra, or digest mismatch); payload names the direction.
    #[error("version bundle does not match desired workers: {0}")]
    BundleMismatch(&'static str),

    /// A facade/egress/service reference names a service not present in
    /// `system_workers`, or a service name collides; payload names the ref.
    #[error("unresolvable service reference: {0}")]
    ServiceReference(&'static str),

    /// A digest-valued field (bundle digest, module digest, system module
    /// digest) is not 64 lowercase hex chars; payload names the field.
    #[error("malformed digest: {0}")]
    Digest(&'static str),

    /// A run-parameter bound was exceeded (var/secret value length);
    /// payload names the parameter.
    #[error("capacity bound exceeded: {0}")]
    Capacity(&'static str),

    /// A tenant `compatibilityFlags[]` entry is not a known stable flag —
    /// either `$experimental`-annotated or unknown to the vendored
    /// `compatibility-date.capnp` snapshot. Fail closed: with
    /// `--experimental` on the argv (consumer generations) a slipped-in
    /// experimental flag would actually apply. System/facade workers are
    /// exempt (platform-emitted). Payload names the offending flag…'s
    /// category (`compatFlag.experimental` / `compatFlag.unknown`).
    #[error("compat flag not allowed for tenants: {0}")]
    CompatFlag(&'static str),

    /// Every worker in the tenant failed its per-worker emit dry-run —
    /// nothing servable would remain, so failing the whole render and
    /// keeping the previous generation is strictly safer than an empty
    /// tenant workerd (W3-r2 fault isolation).
    #[error("all workers excluded from render")]
    AllWorkersExcluded,

    /// A NetworkPolicy `except` CIDR is, or overlaps, the IPv4-mapped
    /// IPv6 range `::ffff:0:0/96`, which the Kubernetes API server
    /// rejects inside `ipBlock.except` (rolling out such a policy
    /// fails). `cidr` is the canonical text of an already-parsed CIDR
    /// (charset-bounded, not raw input). Express the range in IPv4 form.
    #[error("{field}: {cidr} is or overlaps an IPv4-mapped IPv6 range (::ffff:0:0/96), which Kubernetes ipBlock.except rejects; use the IPv4 CIDR form instead")]
    Ipv4MappedExcept { field: &'static str, cidr: String },
}

impl ConfigError {
    /// Stable protocol error code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Identifier(_) => "CONFIG_INVALID_IDENTIFIER",
            Self::UnresolvedReference(_) => "CONFIG_UNRESOLVED_REFERENCE",
            Self::ServiceTarget => "CONFIG_SERVICE_TARGET_SCOPE",
            Self::BundleMismatch(_) => "CONFIG_BUNDLE_MISMATCH",
            Self::ServiceReference(_) => "CONFIG_UNRESOLVED_SERVICE",
            Self::Digest(_) => "CONFIG_INVALID_DIGEST",
            Self::Capacity(_) => "CONFIG_CAPACITY_EXCEEDED",
            Self::CompatFlag(_) => "CONFIG_COMPAT_FLAG",
            Self::AllWorkersExcluded => "CONFIG_ALL_WORKERS_EXCLUDED",
            Self::Ipv4MappedExcept { .. } => "CONFIG_NETPOL_IPV4_MAPPED_EXCEPT",
        }
    }
}
