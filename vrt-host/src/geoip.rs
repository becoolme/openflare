//! Local GeoIP resolution for `request.cf` (MIG-10 follow-up).
//!
//! The Cloudflare `request.cf` object carries geolocation and ASN
//! data describing the CLIENT IP at the edge. Today those fields are
//! only synthesized from a verified `trusted-proxy → Cloudflare-edge`
//! chain's `cf-*` headers, so a deployment without a real Cloudflare
//! front sees none of them. This module resolves the same fields
//! locally instead: after `ClientIpPolicy` derives the client IP (or
//! the TCP peer itself, when no trusted proxy exists — that peer IS
//! the client as far as this edge can verify), a merged MMDB lookup
//! fills the fields upstream did not provide. Local data can never
//! replace verified upstream values: when a real CF edge is in front,
//! its `cf-*` headers describe the same client IP and remain
//! authoritative (they are what the worker would see on Cloudflare
//! itself).
//!
//! Database: a single merged MMDB (`GeoLite2-City` schema plus `asn`
//! and `proxy` subtrees — the NetworkCats/Merged-IP-Data layout). The
//! reader is memory-mapped and swaps on mtime change; [`sync`]
//! refreshes the file in the background. Every failure mode degrades
//! to "no geo data" — a missing/corrupt/stale database never blocks
//! or poisons a request, matching the trust-store precedent.
//!
//! CF-faithful types: `request.cf.asn` is a JSON NUMBER, `latitude`/
//! `longitude` are fixed-precision STRINGS, `metroCode` is a string —
//! [`GeoRecord`] carries them already in emitted form so the blob
//! writer stays a dumb serializer.

use arc_swap::ArcSwapOption;
use serde::Deserialize;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// How often `lookup` may stat the database path for an out-of-band
/// replacement (an operator drop or a sidecar download). The sync
/// task reloads explicitly after each successful fetch — this only
/// covers files it did not write.
const RELOAD_CHECK_SECS: u64 = 60;

/// Geo fields extracted from one MMDB lookup, already in emitted
/// shape: every string is bounded/validated before it can reach
/// `request.cf` or a `cf-*` header.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GeoRecord {
    /// `country.iso_code` (fallback `registered_country`) →
    /// `request.cf.country` + `cf-ipcountry`.
    pub country: Option<String>,
    /// `continent.code` → `request.cf.continent` + `cf-ipcontinent`.
    pub continent: Option<String>,
    /// `city.names.en` → `request.cf.city` + `cf-ipcity`.
    pub city: Option<String>,
    /// First `subdivisions[]` English name → `request.cf.region` +
    /// `cf-region`.
    pub region: Option<String>,
    /// First `subdivisions[]` `iso_code` → `request.cf.regionCode` +
    /// `cf-region-code`.
    pub region_code: Option<String>,
    /// `location.metro_code` (US DMA) → `request.cf.metroCode` +
    /// `cf-metro-code`.
    pub metro_code: Option<String>,
    /// `postal.code` → `request.cf.postalCode` + `cf-postal-code`.
    pub postal_code: Option<String>,
    /// `location.time_zone` (IANA) → `request.cf.timezone` +
    /// `cf-timezone`.
    pub timezone: Option<String>,
    /// `location.latitude` as a fixed-precision string (CF emits
    /// `"30.27130"`) → `request.cf.latitude` + `cf-iplatitude`.
    pub latitude: Option<String>,
    /// Same for longitude → `request.cf.longitude` + `cf-iplongitude`.
    pub longitude: Option<String>,
    /// `asn.autonomous_system_number` → `request.cf.asn` (JSON
    /// number, never a string).
    pub asn: Option<u32>,
    /// `asn.autonomous_system_organization` →
    /// `request.cf.asOrganization`.
    pub as_org: Option<String>,
    /// `proxy.is_tor` — kept for a future platform risk signal; NOT
    /// emitted to `request.cf` (Cloudflare has no equivalent field).
    pub is_tor: bool,
    /// `proxy.is_vpn` — same, future use only.
    pub is_vpn: bool,
    /// `proxy.is_hosting` — same, future use only.
    pub is_hosting: bool,
}

/// Request-path handle around the loaded database. Cheap to clone
/// (one `Arc`); lookups are `O(prefix)` trie walks with no
/// allocation beyond the small fixed record decode.
pub struct GeoIp {
    /// `None` = no usable database right now (absent, corrupt, or not
    /// yet downloaded). Requests degrade to upstream-verified fields.
    /// `Vec<u8>` backing, NOT mmap: `open_mmap` is `unsafe` because an
    /// in-place truncation of a mapped file is UB; holding the ~100 MB
    /// image in heap makes a hostile or careless overwrite a decode
    /// error instead of a crash, and this host has exactly one reader.
    reader: ArcSwapOption<maxminddb::Reader<Vec<u8>>>,
    path: PathBuf,
    /// mtime (unix secs) of the file the current reader was opened
    /// from — `u64::MAX` when nothing is loaded.
    mtime_secs: AtomicU64,
    /// unix secs of the last path stat (throttles the syscall to
    /// [`RELOAD_CHECK_SECS`]).
    checked_at: AtomicU64,
}

impl GeoIp {
    /// Open `path` if a valid database is already there; absent or
    /// unreadable starts empty (the sync task will fill it).
    pub fn open(path: PathBuf) -> Arc<Self> {
        let geo = Arc::new(Self {
            reader: ArcSwapOption::from(None),
            path,
            mtime_secs: AtomicU64::new(u64::MAX),
            checked_at: AtomicU64::new(0),
        });
        geo.reload();
        geo
    }

    /// Look up `ip`. `None` when no database is loaded or the address
    /// has no geo record — never an error on the request path.
    pub fn lookup(&self, ip: IpAddr) -> Option<GeoRecord> {
        self.maybe_reload();
        let reader = self.reader.load_full()?;
        let result = reader.lookup(ip).ok()?;
        let record: Option<MmdbRecord> = result.decode().ok().flatten();
        record.map(|r| r.into_geo())
    }

    /// Re-open the database file and swap the reader. Called by the
    /// sync task after a successful download and lazily by `lookup`.
    /// An atomic rename means an open either sees the whole old file
    /// or the whole new file; an open error keeps the CURRENT reader
    /// (stale geo beats none), except ENOENT which drops it — a
    /// deliberate removal is honored.
    pub fn reload(&self) {
        match self.path.metadata() {
            Ok(md) => {
                let mtime = md
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                match maxminddb::Reader::open_readfile(&self.path) {
                    Ok(reader) => {
                        self.reader.store(Some(Arc::new(reader)));
                        self.mtime_secs.store(mtime, Ordering::Relaxed);
                        crate::metrics::registry().set("vrt_host_geoip_loaded", &[], 1.0);
                    }
                    Err(e) => {
                        tracing::warn!(path = %self.path.display(), error = %e,
                            "geoip: mmdb open failed — keeping previous database");
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if self.reader.swap(None).is_some() {
                    tracing::info!(path = %self.path.display(), "geoip: mmdb removed — geo fields dropped");
                }
                self.mtime_secs.store(u64::MAX, Ordering::Relaxed);
                crate::metrics::registry().set("vrt_host_geoip_loaded", &[], 0.0);
            }
            Err(e) => {
                tracing::warn!(path = %self.path.display(), error = %e,
                    "geoip: mmdb stat failed — keeping previous database");
            }
        }
    }

    /// Stat the path at most once per [`RELOAD_CHECK_SECS`]; reload
    /// when the mtime moved. One `AtomicU64` load per request, one
    /// stat per minute — the mmap swap itself is lock-free.
    fn maybe_reload(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let last = self.checked_at.load(Ordering::Relaxed);
        if now.saturating_sub(last) < RELOAD_CHECK_SECS {
            return;
        }
        if self
            .checked_at
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let mtime = self
            .path
            .metadata()
            .ok()
            .and_then(|md| md.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs());
        match (mtime, self.mtime_secs.load(Ordering::Relaxed)) {
            (Some(m), cur) if m != cur => self.reload(),
            (None, cur) if cur != u64::MAX => self.reload(),
            _ => {}
        }
    }

    /// Database path — the sync task writes its atomic rename here.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

// ---- MMDB record decoding (merged-database layout) ----

#[derive(Deserialize)]
struct MmdbNames<'a> {
    #[serde(borrow)]
    en: Option<&'a str>,
}

#[derive(Deserialize)]
struct MmdbCountry<'a> {
    #[serde(borrow)]
    iso_code: Option<&'a str>,
}

#[derive(Deserialize)]
struct MmdbContinent<'a> {
    #[serde(borrow)]
    code: Option<&'a str>,
}

/// `city`/`subdivisions[]`/`registered_country` all carry a `names`
/// map — only the English name reaches `request.cf`.
#[derive(Deserialize)]
struct MmdbNamed<'a> {
    #[serde(borrow)]
    names: Option<MmdbNames<'a>>,
}

#[derive(Deserialize)]
struct MmdbSub<'a> {
    #[serde(borrow)]
    iso_code: Option<&'a str>,
    #[serde(borrow)]
    names: Option<MmdbNames<'a>>,
}

#[derive(Deserialize)]
struct MmdbPostal<'a> {
    #[serde(borrow)]
    code: Option<&'a str>,
}

#[derive(Deserialize)]
struct MmdbLocation<'a> {
    latitude: Option<f64>,
    longitude: Option<f64>,
    metro_code: Option<u16>,
    #[serde(borrow)]
    time_zone: Option<&'a str>,
}

#[derive(Deserialize)]
struct MmdbAsn<'a> {
    autonomous_system_number: Option<u32>,
    #[serde(borrow)]
    autonomous_system_organization: Option<&'a str>,
}

#[derive(Deserialize)]
struct MmdbProxy {
    is_tor: Option<bool>,
    is_vpn: Option<bool>,
    is_hosting: Option<bool>,
}

#[derive(Deserialize)]
struct MmdbRecord<'a> {
    #[serde(borrow)]
    country: Option<MmdbCountry<'a>>,
    #[serde(borrow)]
    registered_country: Option<MmdbCountry<'a>>,
    #[serde(borrow)]
    continent: Option<MmdbContinent<'a>>,
    #[serde(borrow)]
    city: Option<MmdbNamed<'a>>,
    #[serde(borrow)]
    subdivisions: Option<Vec<MmdbSub<'a>>>,
    #[serde(borrow)]
    location: Option<MmdbLocation<'a>>,
    #[serde(borrow)]
    postal: Option<MmdbPostal<'a>>,
    #[serde(borrow)]
    asn: Option<MmdbAsn<'a>>,
    proxy: Option<MmdbProxy>,
}

impl MmdbRecord<'_> {
    /// Map the MMDB record onto emitted shapes. Every string passes
    /// the same bounded/control-free validation the upstream-header
    /// path applies (`crate::ingress::cf_header_text` semantics),
    /// duplicated here because the values come from a file, not a
    /// header map.
    fn into_geo(self) -> GeoRecord {
        let country = self
            .country
            .and_then(|c| c.iso_code)
            .or_else(|| self.registered_country.and_then(|c| c.iso_code))
            .and_then(|s| text(s, 2));
        let continent = self.continent.and_then(|c| c.code).and_then(|s| text(s, 2));
        let city = self
            .city
            .and_then(|c| c.names)
            .and_then(|n| n.en)
            .and_then(|s| text(s, 96));
        let first_sub = self.subdivisions.and_then(|s| s.into_iter().next());
        let region = first_sub
            .as_ref()
            .and_then(|s| s.names.as_ref())
            .and_then(|n| n.en)
            .and_then(|s| text(s, 96));
        let region_code = first_sub.and_then(|s| s.iso_code).and_then(|s| text(s, 16));
        let metro_code = self
            .location
            .as_ref()
            .and_then(|l| l.metro_code)
            .map(|m| m.to_string());
        let postal_code = self.postal.and_then(|p| p.code).and_then(|s| text(s, 32));
        let timezone = self
            .location
            .as_ref()
            .and_then(|l| l.time_zone)
            .and_then(|s| text(s, 64));
        let latitude = self
            .location
            .as_ref()
            .and_then(|l| l.latitude)
            .and_then(|v| coord(v, -90.0, 90.0));
        let longitude = self
            .location
            .and_then(|l| l.longitude)
            .and_then(|v| coord(v, -180.0, 180.0));
        let asn = self.asn.as_ref().and_then(|a| a.autonomous_system_number);
        let as_org = self
            .asn
            .and_then(|a| a.autonomous_system_organization)
            .and_then(|s| text(s, 128));
        let proxy = self.proxy.unwrap_or(MmdbProxy {
            is_tor: None,
            is_vpn: None,
            is_hosting: None,
        });
        GeoRecord {
            country,
            continent,
            city,
            region,
            region_code,
            metro_code,
            postal_code,
            timezone,
            latitude,
            longitude,
            asn,
            as_org,
            is_tor: proxy.is_tor.unwrap_or(false),
            is_vpn: proxy.is_vpn.unwrap_or(false),
            is_hosting: proxy.is_hosting.unwrap_or(false),
        }
    }
}

/// Bounded, control-free text — same contract as
/// `crate::ingress::cf_header_text` (max length, no control chars,
/// non-empty). Values from the MMDB are treated as untrusted input.
fn text(s: &str, max: usize) -> Option<String> {
    let s = s.trim();
    if s.is_empty() || s.len() > max || s.chars().any(|c| c.is_control()) {
        return None;
    }
    Some(s.to_string())
}

/// CF formats coordinates as fixed-5-decimal strings; drop values
/// outside the axis range.
fn coord(v: f64, min: f64, max: f64) -> Option<String> {
    if !v.is_finite() || !(min..=max).contains(&v) {
        return None;
    }
    Some(format!("{v:.5}"))
}

pub mod sync;

#[cfg(test)]
mod tests {
    use super::*;

    /// A merged-database-shaped JSON record decodes into the emitted
    /// `GeoRecord` — string fields bounded, asn numeric, coords
    /// fixed-precision.
    #[test]
    fn mmdb_record_maps_to_geo() {
        let json = r#"{
            "country": {"iso_code": "JP"},
            "continent": {"code": "AS"},
            "city": {"names": {"en": "Tokyo", "de": "Tokio"}},
            "subdivisions": [{"iso_code": "13", "names": {"en": "Tokyo"}}],
            "postal": {"code": "100-0001"},
            "location": {
                "latitude": 35.6895,
                "longitude": 139.6917,
                "metro_code": 0,
                "time_zone": "Asia/Tokyo"
            },
            "asn": {
                "autonomous_system_number": 2516,
                "autonomous_system_organization": "KDDI CORPORATION"
            },
            "proxy": {"is_tor": false, "is_vpn": true}
        }"#;
        let r: MmdbRecord = serde_json::from_str(json).unwrap();
        let g = r.into_geo();
        assert_eq!(g.country.as_deref(), Some("JP"));
        assert_eq!(g.continent.as_deref(), Some("AS"));
        assert_eq!(g.city.as_deref(), Some("Tokyo"));
        assert_eq!(g.region.as_deref(), Some("Tokyo"));
        assert_eq!(g.region_code.as_deref(), Some("13"));
        assert_eq!(g.postal_code.as_deref(), Some("100-0001"));
        assert_eq!(g.timezone.as_deref(), Some("Asia/Tokyo"));
        assert_eq!(g.latitude.as_deref(), Some("35.68950"));
        assert_eq!(g.longitude.as_deref(), Some("139.69170"));
        assert_eq!(g.asn, Some(2516));
        assert_eq!(g.as_org.as_deref(), Some("KDDI CORPORATION"));
        assert!(g.is_vpn && !g.is_tor && !g.is_hosting);
    }

    /// `registered_country` is the fallback when `country` is absent.
    #[test]
    fn registered_country_fallback() {
        let r: MmdbRecord =
            serde_json::from_str(r#"{"registered_country": {"iso_code": "US"}}"#).unwrap();
        assert_eq!(r.into_geo().country.as_deref(), Some("US"));
    }

    /// Absent subtrees produce `None`, never empty strings; NaN and
    /// out-of-range coordinates are dropped.
    #[test]
    fn sparse_and_invalid_values() {
        let r: MmdbRecord =
            serde_json::from_str(r#"{"location": {"latitude": 95.0, "longitude": 0.0}}"#).unwrap();
        let g = r.into_geo();
        assert!(g.latitude.is_none()); // 95 > 90 range bound
        assert_eq!(g.longitude.as_deref(), Some("0.00000"));
        assert!(g.country.is_none() && g.asn.is_none() && !g.is_tor);
    }

    /// An absent database file is a first-class state: `open` does
    /// not error, lookups return `None`, and a garbage file dropped
    /// in place still yields `None` (never a partial decode).
    #[test]
    fn missing_and_garbage_files_yield_none() {
        let dir = std::env::temp_dir().join(format!("vrt-geoip-test-{}", std::process::id()));
        let path = dir.join("missing.mmdb");
        let geo = GeoIp::open(path.clone());
        assert!(geo
            .lookup(IpAddr::V4(std::net::Ipv4Addr::new(8, 8, 8, 8)))
            .is_none());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&path, b"not an mmdb").unwrap();
        geo.reload();
        assert!(geo
            .lookup(IpAddr::V4(std::net::Ipv4Addr::new(8, 8, 8, 8)))
            .is_none());
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    /// Full pipeline check against the real merged database — gated
    /// like `VRT_WORKERD_BIN`: set `VRT_GEOIP_MMDB=/path/Merged-IP.mmdb`.
    #[test]
    fn real_mmdb_lookup() {
        let Some(path) = std::env::var_os("VRT_GEOIP_MMDB") else {
            eprintln!("skipping: VRT_GEOIP_MMDB unset");
            return;
        };
        let geo = GeoIp::open(PathBuf::from(path));
        // Cloudflare's own anycast resolver is in every geo database.
        let rec = geo
            .lookup(IpAddr::V4(std::net::Ipv4Addr::new(1, 1, 1, 1)))
            .expect("1.1.1.1 must resolve in a real mmdb");
        assert_eq!(rec.country.as_deref(), Some("AU"));
        assert!(rec.asn.is_some());
    }
}
