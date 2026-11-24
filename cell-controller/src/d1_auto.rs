//! Opt-in automatic D1 writer failover.
//!
//! The cell probes the configured writer's `/healthz`. After
//! `failure_threshold` consecutive failures it calls the follower promote
//! endpoint (the same CAS/adoption as a manual `PROMOTE` op) and reports the
//! resulting epoch/lsn/writer address to the control plane, which republishes
//! the signed desired document. Promotion remains lease-aware because the
//! follower CAS refuses a live manifest head.
//!
//! Off unless `[d1sql].auto_failover` entries exist. There is no quorum: the
//! object-store manifest CAS is the single-writer authority.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use vrt_provider::d1::SqlAuthSigner;

/// One database's automatic-failover policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoFailoverConfig {
    /// 26-char D1 database id.
    pub database_id: String,
    /// Writer health endpoint base (`GET {writer_probe}/healthz`).
    pub writer_probe: String,
    /// Advertised serving endpoint of the new writer (published in desired).
    pub writer_addr: String,
    /// Follower promote endpoint base (`POST {promote_endpoint}/internal/dbs/{id}/promote`).
    pub promote_endpoint: String,
    /// Consecutive failed probes before promotion (default 3).
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    /// Minimum time between promotion attempts (default 300s).
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u64,
    /// Probe cadence (default 10s).
    #[serde(default = "default_probe_interval_secs")]
    pub probe_interval_secs: u64,
}

fn default_failure_threshold() -> u32 {
    3
}
fn default_cooldown_secs() -> u64 {
    300
}
fn default_probe_interval_secs() -> u64 {
    10
}

impl AutoFailoverConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.database_id.len() != 26
            || !self
                .database_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        {
            return Err("auto_failover.database_id must be 26 lowercase alphanumerics".into());
        }
        for (name, url) in [
            ("writer_probe", &self.writer_probe),
            ("writer_addr", &self.writer_addr),
            ("promote_endpoint", &self.promote_endpoint),
        ] {
            crate::d1ops::validate_data_plane_endpoint(url)
                .map_err(|e| format!("auto_failover.{name} {e}"))?;
        }
        if !(1..=20).contains(&self.failure_threshold) {
            return Err("auto_failover.failure_threshold out of range [1,20]".into());
        }
        if !(30..=86_400).contains(&self.cooldown_secs) {
            return Err("auto_failover.cooldown_secs out of range [30,86400]".into());
        }
        if !(2..=300).contains(&self.probe_interval_secs) {
            return Err("auto_failover.probe_interval_secs out of range [2,300]".into());
        }
        Ok(())
    }

    pub fn probe_interval(&self) -> Duration {
        Duration::from_secs(self.probe_interval_secs)
    }
}

/// Promotion result returned by the follower endpoint.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Promotion {
    pub epoch: u64,
    #[serde(default)]
    pub lsn: Option<u64>,
    #[serde(default)]
    pub resumed: bool,
}

/// External side effects of one probe/promote/report cycle.
#[async_trait]
pub trait AutoFailoverDeps: Send + Sync {
    async fn healthy(&self, writer_probe: &str) -> bool;
    async fn promote(&self, promote_endpoint: &str, database_id: &str) -> Result<Promotion, String>;
    async fn report(
        &self,
        database_id: &str,
        epoch: u64,
        lsn: Option<u64>,
        writer_addr: &str,
    ) -> Result<(), String>;
}

/// Observable result of one tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AutoOutcome {
    Healthy,
    Failed { consecutive: u32 },
    Promoted { epoch: u64 },
    PromoteFailed,
    Cooling,
}

/// Per-database state machine.
pub struct AutoFailover {
    cfg: AutoFailoverConfig,
    failures: u32,
    last_promote: Option<Instant>,
}

impl AutoFailover {
    pub fn new(cfg: AutoFailoverConfig) -> Self {
        Self {
            cfg,
            failures: 0,
            last_promote: None,
        }
    }

    pub fn config(&self) -> &AutoFailoverConfig {
        &self.cfg
    }

    pub async fn tick(&mut self, deps: &dyn AutoFailoverDeps, now: Instant) -> AutoOutcome {
        if deps.healthy(&self.cfg.writer_probe).await {
            self.failures = 0;
            return AutoOutcome::Healthy;
        }
        crate::metrics::d1_auto("probe_failed");
        self.failures = self.failures.saturating_add(1);
        let cooldown = Duration::from_secs(self.cfg.cooldown_secs);
        if self
            .last_promote
            .is_some_and(|at| now.saturating_duration_since(at) < cooldown)
        {
            crate::metrics::d1_auto("cooling");
            return AutoOutcome::Cooling;
        }
        if self.failures < self.cfg.failure_threshold {
            return AutoOutcome::Failed {
                consecutive: self.failures,
            };
        }
        // Mark the attempt before the network call: a hung promote endpoint
        // must not turn every probe into another promotion.
        self.last_promote = Some(now);
        self.failures = 0;
        match deps
            .promote(&self.cfg.promote_endpoint, &self.cfg.database_id)
            .await
        {
            Ok(promotion) => {
                if let Err(e) = deps
                    .report(
                        &self.cfg.database_id,
                        promotion.epoch,
                        promotion.lsn,
                        &self.cfg.writer_addr,
                    )
                    .await
                {
                    tracing::error!(
                        db = %self.cfg.database_id,
                        epoch = promotion.epoch,
                        error = %e,
                        "d1 auto-failover promoted but the control-plane report failed"
                    );
                    crate::metrics::d1_auto("promote_failed");
                    return AutoOutcome::PromoteFailed;
                }
                crate::metrics::d1_auto("promoted");
                tracing::warn!(
                    db = %self.cfg.database_id,
                    epoch = promotion.epoch,
                    lsn = promotion.lsn,
                    "d1 auto-failover promoted a new writer"
                );
                AutoOutcome::Promoted {
                    epoch: promotion.epoch,
                }
            }
            Err(e) => {
                crate::metrics::d1_auto("promote_failed");
                tracing::error!(db = %self.cfg.database_id, error = %e,
                    "d1 auto-failover promotion failed");
                AutoOutcome::PromoteFailed
            }
        }
    }
}

/// Production HTTP deps.
pub struct HttpAutoDeps {
    client: reqwest::Client,
    signer: SqlAuthSigner,
    control_plane: String,
    cell_id: String,
    token: String,
}

impl HttpAutoDeps {
    pub fn new(
        control_plane: &str,
        cell_id: &str,
        token: String,
        auth_secret: Vec<u8>,
    ) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| format!("auto-failover http client: {e}"))?;
        Ok(Self {
            client,
            signer: SqlAuthSigner::new(auth_secret),
            control_plane: control_plane.trim_end_matches('/').to_string(),
            cell_id: cell_id.to_string(),
            token,
        })
    }
}

#[async_trait]
impl AutoFailoverDeps for HttpAutoDeps {
    async fn healthy(&self, writer_probe: &str) -> bool {
        self.client
            .get(format!("{}/healthz", writer_probe.trim_end_matches('/')))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    async fn promote(&self, promote_endpoint: &str, database_id: &str) -> Result<Promotion, String> {
        let path = format!("/internal/dbs/{database_id}/promote");
        let body = b"{}".to_vec();
        let auth = self.signer.sign("POST", &path, &body, 0, 0);
        let resp = self
            .client
            .post(format!("{}{path}", promote_endpoint.trim_end_matches('/')))
            .header(vrt_provider::d1::SQL_AUTH_HEADER, auth)
            .header(vrt_provider::d1::SQL_EPOCH_HEADER, "0")
            .header(vrt_provider::d1::SQL_SEQ_HEADER, "0")
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| format!("promote transport: {e}"))?;
        let status = resp.status();
        let value: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("promote body: {e}"))?;
        if !status.is_success() || value.get("success") == Some(&serde_json::json!(false)) {
            return Err(format!("promote status {status}: {value}"));
        }
        serde_json::from_value(value).map_err(|e| format!("promote decode: {e}"))
    }

    async fn report(
        &self,
        database_id: &str,
        epoch: u64,
        lsn: Option<u64>,
        writer_addr: &str,
    ) -> Result<(), String> {
        let url = format!(
            "{}/api/internal/clusters/{}/vrt/d1-writer",
            self.control_plane, self.cell_id
        );
        let resp = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({
                "databaseId": database_id,
                "epoch": epoch,
                "lsn": lsn,
                "writerAddr": writer_addr,
                "cause": "D1_AUTO_PROMOTE",
            }))
            .send()
            .await
            .map_err(|e| format!("writer report transport: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("writer report status {}", resp.status()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct Mock {
        healthy: AtomicBool,
        promotes: AtomicUsize,
        reports: AtomicUsize,
        promote_ok: bool,
    }

    #[async_trait]
    impl AutoFailoverDeps for Mock {
        async fn healthy(&self, _writer_probe: &str) -> bool {
            self.healthy.load(Ordering::SeqCst)
        }
        async fn promote(
            &self,
            _promote_endpoint: &str,
            _database_id: &str,
        ) -> Result<Promotion, String> {
            self.promotes.fetch_add(1, Ordering::SeqCst);
            if self.promote_ok {
                Ok(Promotion {
                    epoch: 2,
                    lsn: Some(42),
                    resumed: false,
                })
            } else {
                Err("boom".into())
            }
        }
        async fn report(
            &self,
            _database_id: &str,
            _epoch: u64,
            _lsn: Option<u64>,
            _writer_addr: &str,
        ) -> Result<(), String> {
            self.reports.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn cfg() -> AutoFailoverConfig {
        AutoFailoverConfig {
            database_id: "a".repeat(26),
            writer_probe: "http://127.0.0.1:1".into(),
            writer_addr: "http://127.0.0.1:2".into(),
            promote_endpoint: "http://127.0.0.1:3".into(),
            failure_threshold: 2,
            cooldown_secs: 30,
            probe_interval_secs: 5,
        }
    }

    fn deps(healthy: bool, promote_ok: bool) -> Mock {
        Mock {
            healthy: AtomicBool::new(healthy),
            promotes: AtomicUsize::new(0),
            reports: AtomicUsize::new(0),
            promote_ok,
        }
    }

    #[tokio::test]
    async fn promotes_only_after_threshold_and_reports() {
        let mut auto = AutoFailover::new(cfg());
        let d = deps(false, true);
        let now = Instant::now();
        assert!(matches!(
            auto.tick(&d, now).await,
            AutoOutcome::Failed { consecutive: 1 }
        ));
        assert_eq!(d.promotes.load(Ordering::SeqCst), 0);
        assert!(matches!(
            auto.tick(&d, now + Duration::from_secs(1)).await,
            AutoOutcome::Promoted { epoch: 2 }
        ));
        assert_eq!(d.promotes.load(Ordering::SeqCst), 1);
        assert_eq!(d.reports.load(Ordering::SeqCst), 1);
        // Cooldown suppresses an immediate second promotion.
        assert!(matches!(
            auto.tick(&d, now + Duration::from_secs(2)).await,
            AutoOutcome::Cooling
        ));
    }

    #[tokio::test]
    async fn healthy_probe_resets_the_failure_streak() {
        let mut auto = AutoFailover::new(cfg());
        let d = deps(false, true);
        let now = Instant::now();
        auto.tick(&d, now).await;
        let healthy = deps(true, true);
        assert!(matches!(
            auto.tick(&healthy, now + Duration::from_secs(1)).await,
            AutoOutcome::Healthy
        ));
        let d = deps(false, true);
        assert!(matches!(
            auto.tick(&d, now + Duration::from_secs(2)).await,
            AutoOutcome::Failed { consecutive: 1 }
        ));
    }

    #[tokio::test]
    async fn failed_promotion_does_not_report_and_cools_down() {
        let mut auto = AutoFailover::new(cfg());
        let d = deps(false, false);
        let now = Instant::now();
        auto.tick(&d, now).await;
        assert_eq!(
            auto.tick(&d, now + Duration::from_secs(1)).await,
            AutoOutcome::PromoteFailed
        );
        assert_eq!(d.reports.load(Ordering::SeqCst), 0);
        assert_eq!(
            auto.tick(&d, now + Duration::from_secs(2)).await,
            AutoOutcome::Cooling
        );
    }

    #[test]
    fn config_is_bounded() {
        assert!(cfg().validate().is_ok());
        let mut bad = cfg();
        bad.database_id = "short".into();
        assert!(bad.validate().is_err());
        let mut bad = cfg();
        bad.failure_threshold = 0;
        assert!(bad.validate().is_err());
        let mut bad = cfg();
        bad.cooldown_secs = 1;
        assert!(bad.validate().is_err());
    }
}
