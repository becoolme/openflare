interface ImportMetaEnv {
  /** Site domain reported to analytics, e.g. "openflare.app". */
  readonly PUBLIC_ANALYTICS_DOMAIN?: string;
  /** URL of a Plausible-compatible analytics script. */
  readonly PUBLIC_ANALYTICS_SRC?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
