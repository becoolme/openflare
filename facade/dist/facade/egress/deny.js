import { WorkerEntrypoint } from "cloudflare:workers";
import { DENIED_HEADER, DENY_CODE } from "./shared.js";
export class DenyAll extends WorkerEntrypoint {
    async fetch() {
        return new Response(JSON.stringify({
            error: "EGRESS_DENIED",
            code: DENY_CODE
        }), {
            status: 403,
            headers: {
                "content-type": "application/json",
                "cache-control": "no-store",
                [DENIED_HEADER]: DENY_CODE
            }
        });
    }
    async connect(socket) {
        try {
            await socket?.close?.();
        } catch  {}
        throw Object.assign(new Error("vrt-no-network denies connect()"), {
            stableCode: "EGRESS_DENIED",
            denyCode: "connect"
        });
    }
}
export default DenyAll;
