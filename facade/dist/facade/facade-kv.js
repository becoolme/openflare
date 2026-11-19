import { KVNamespace as VendoredKVNamespace } from "../third_party/open-compute/facade/kv/transport.js";
import { envWithBackend } from "./props.js";
export class KVNamespace extends VendoredKVNamespace {
    constructor(ctx, env){
        super(ctx, envWithBackend(env, ctx?.props));
    }
}
export default KVNamespace;
