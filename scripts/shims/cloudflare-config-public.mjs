// Workers-safe stand-in for `@cloudflare/config`.
//
// The package root entry registers Node module hooks and imports
// `node:module`; `./public` is pure data/helper code (defineConfig,
// bindings, triggers, exports) and is the only piece a config module
// needs at evaluation time. The sandbox aliases the bare specifier to
// this file so a `cloudflare.config.ts` bundles without the Node loader.
export * from "@cloudflare/config/public";
