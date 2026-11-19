// facade-kv system worker entry: vendored KVNamespace entrypoint plus the
// vrt props → identity-header adapter (see props.ts). The tenant `kv`
// binding targets this service with entrypoint "KVNamespace".
import { KVNamespace as VendoredKVNamespace } from "../third_party/open-compute/facade/kv/transport.js";
import { envWithBackend } from "./props.js";

export class KVNamespace extends VendoredKVNamespace {
  constructor(ctx: { props?: unknown }, env: unknown) {
    super(ctx, envWithBackend(env, ctx?.props));
  }
}
export default KVNamespace;
