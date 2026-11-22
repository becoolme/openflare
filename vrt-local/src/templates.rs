//! Render helpers shared by `of init` and `of dev`.

/// Inject the dev-only `persistence.allowSameDeviceDurability = true`
/// exemption into a rendered `vrt-sql.toml`.
///
/// The durability guard (`VRT_SQL_DURABILITY_SAME_DEVICE`) is
/// fail-closed on purpose: the fs persistence root must live on a
/// different device from the SQLite db so that losing one disk cannot
/// destroy both copies. Single-machine dev stacks (`of dev`, the
/// `deploy/standalone` compose quick start, CI) place both under one
/// device by construction, so they opt out explicitly and loudly.
///
/// Never call this for a production render: production configs ship
/// without the key and the guard stays armed.
pub fn dev_allow_same_device_durability(sql_toml: &str) -> String {
    const ANCHOR: &str = "[persistence]\n";
    const INJECT: &str = "[persistence]\n\
        # DEV ONLY (of dev / deploy-standalone quick start): single device,\n\
        # no disaster recovery. Production must leave this unset and keep the\n\
        # persistence root on a separate device (or use the s3 backend).\n\
        allowSameDeviceDurability = true\n";
    sql_toml.replacen(ANCHOR, INJECT, 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEMPLATE: &str = include_str!("../../examples/standalone-vrt-sql.toml");

    #[test]
    fn template_has_the_persistence_anchor() {
        assert!(TEMPLATE.contains("[persistence]\n"));
    }

    #[test]
    fn injects_the_dev_only_key_once_under_persistence() {
        let out = dev_allow_same_device_durability(TEMPLATE);
        assert_eq!(out.matches("allowSameDeviceDurability").count(), 1);
        let at = out.find("allowSameDeviceDurability").unwrap();
        let section = out[..at].rfind('[').map(|i| &out[i..at]).unwrap_or("");
        assert!(section.contains("[persistence]"), "{section:?}");
        assert!(out.contains("DEV ONLY"));
    }

    #[test]
    fn no_anchor_means_no_change() {
        // A template without [persistence] must never gain the key by
        // accident (fail closed beats a silent exemption).
        assert_eq!(
            dev_allow_same_device_durability("dbRoot = \"/x\"\n"),
            "dbRoot = \"/x\"\n"
        );
    }
}
