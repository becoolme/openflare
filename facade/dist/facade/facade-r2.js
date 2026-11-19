import { makeR2TransportBase } from "../third_party/open-compute/facade/r2/transport.js";
import { bindingError } from "../third_party/open-compute/facade/loader/shared.js";
import { envWithBackend } from "./props.js";
const R2TransportBase = makeR2TransportBase(bindingError);
export class R2Transport extends R2TransportBase {
    constructor(ctx, env){
        super(ctx, envWithBackend(env, ctx?.props));
    }
}
export default R2Transport;
