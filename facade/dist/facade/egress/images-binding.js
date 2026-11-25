import { evaluateTarget } from "./policy.js";
import { SRC_MAX } from "./cf-image.js";
import { stripInternalHeaders } from "./shared.js";
export const IMAGES_BINDING_HOST = "js.images.cloudflare.com";
const FACADE_BASE = "http://vrt-images.internal";
function bindingError(code, status) {
    return new Response(`ERROR ${code}\n`, {
        status,
        headers: {
            "content-type": "text/plain",
            "cache-control": "no-store",
            "cf-images-binding": `err=${code}`
        }
    });
}
function specFontUrl(spec) {
    try {
        const font = JSON.parse(spec).font;
        return typeof font?.url === "string" ? font.url : undefined;
    } catch  {
        return undefined;
    }
}
async function fetchFont(url, policy) {
    if (!evaluateTarget(url, policy).allow) return bindingError(9420, 502);
    try {
        const resp = await fetch(url, {
            signal: AbortSignal.timeout(policy?.requestTimeoutMs ?? 30_000)
        });
        const buf = new Uint8Array(await resp.arrayBuffer());
        if (buf.length > SRC_MAX) return bindingError(9424, 413);
        return buf;
    } catch  {
        return bindingError(9420, 502);
    }
}
function relayResponse(resp) {
    const headers = stripInternalHeaders(resp.headers);
    headers.delete("content-length");
    return new Response(resp.body, {
        status: resp.status,
        statusText: resp.statusText,
        headers
    });
}
async function tryFetch(svc, target, init) {
    try {
        return relayResponse(await svc.fetch(target, init));
    } catch  {
        return bindingError(9523, 500);
    }
}
export async function relayImagesBinding(request, svc, policy) {
    const url = new URL(request.url);
    const target = FACADE_BASE + url.pathname + url.search;
    const init = {
        method: request.method
    };
    const contentType = request.headers.get("content-type");
    if (contentType !== null) {
        init.headers = {
            "content-type": contentType
        };
    }
    let body;
    if (request.method !== "GET" && request.method !== "HEAD") {
        if (url.pathname === "/transform") {
            body = new Uint8Array(await request.arrayBuffer());
            init.body = body;
        } else {
            init.body = request.body ?? undefined;
        }
    }
    if (body !== undefined) {
        const flat = new TextDecoder("latin1").decode(body);
        if (flat.includes('name="text_input"') || flat.includes('name="draw_text"')) {
            let form;
            try {
                form = await new Response(body, {
                    headers: {
                        "content-type": contentType ?? ""
                    }
                }).formData();
            } catch  {
                return tryFetch(svc, target, init);
            }
            const out = new FormData();
            for (const [name, value] of form){
                out.append(name, value);
                if ((name === "text_input" || name === "draw_text") && typeof value === "string") {
                    const fontUrl = specFontUrl(value);
                    if (fontUrl === undefined) continue;
                    const font = await fetchFont(fontUrl, policy);
                    if (font instanceof Response) return font;
                    out.append(name === "text_input" ? "text_font" : "draw_font", new Blob([
                        font
                    ]), "font");
                }
            }
            return tryFetch(svc, target, {
                method: "POST",
                body: out
            });
        }
    }
    return tryFetch(svc, target, init);
}
