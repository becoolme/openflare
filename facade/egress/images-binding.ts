// Relay for the images binding's inner `fetcher`. The upstream
// images-api shim POSTs to https://js.images.cloudflare.com/*; vrt-
// config points the wrapped binding's fetcher at this egress worker
// instead of facade-images directly so that `/transform` bodies
// carrying `text_input`/`draw_text` JSON specs can have their
// `font.url` resolved through the egress policy into adjacent
// `text_font`/`draw_font` file fields first — facade-images is
// deny-routed and cannot fetch fonts itself (the upstream service
// resolved font URLs service-side too). Bodies without text specs,
// and every other path (/info, ...), forward as raw bytes — no
// multipart parsing on the passthrough path.
//
// Errors use the `cf-images-binding: err=<code>` header family the
// facade emits and the shim's throwErrorIfErrorResponse reads; codes
// mirror the draw-payload semantics in egress.ts (unfetchable → 9420,
// oversized → 9424, transport → 9523).
//
// Note the intercept is by hostname only — a tenant hand-crafting a
// request to the same synthetic host reaches the facade too. That is
// upstream-identical (js.images.cloudflare.com answers any caller)
// and strictly better than before, when such fetches egressed to the
// real Cloudflare service.
import { evaluateTarget, type EgressPolicy } from "./policy.js";
import { SRC_MAX } from "./cf-image.js";
import { stripInternalHeaders } from "./shared.js";

export const IMAGES_BINDING_HOST = "js.images.cloudflare.com";
const FACADE_BASE = "http://vrt-images.internal";

export interface ImagesFacade {
  fetch(input: string, init?: RequestInit): Promise<Response>;
}

function bindingError(code: number, status: number): Response {
  return new Response(`ERROR ${code}\n`, {
    status,
    headers: {
      "content-type": "text/plain",
      "cache-control": "no-store",
      "cf-images-binding": `err=${code}`,
    },
  });
}

// The spec's font descriptor — the shim serializes `font:{url}` into
// the text spec JSON. Absent/unparseable specs get no injection and
// facade-images' own 9423 pairing checks reject the request, the same
// outcome as a spec the facade parsed directly.
function specFontUrl(spec: string): string | undefined {
  try {
    const font = (JSON.parse(spec) as { font?: { url?: unknown } }).font;
    return typeof font?.url === "string" ? font.url : undefined;
  } catch {
    return undefined;
  }
}

// Same fetch shape as the draw-payload loop in egress.ts: the target
// re-runs the egress policy, any response body is treated as font
// bytes (a 404 HTML page fails fontdb parsing at the facade → 9420),
// and SRC_MAX caps the wire like the other payload fields.
async function fetchFont(
  url: string,
  policy: EgressPolicy | null,
): Promise<Uint8Array | Response> {
  if (!evaluateTarget(url, policy).allow) return bindingError(9420, 502);
  try {
    const resp = await fetch(url, {
      signal: AbortSignal.timeout(policy?.requestTimeoutMs ?? 30_000),
    });
    const buf = new Uint8Array(await resp.arrayBuffer());
    if (buf.length > SRC_MAX) return bindingError(9424, 413);
    return buf;
  } catch {
    return bindingError(9420, 502);
  }
}

function relayResponse(resp: Response): Response {
  const headers = stripInternalHeaders(resp.headers);
  headers.delete("content-length");
  return new Response(resp.body, {
    status: resp.status,
    statusText: resp.statusText,
    headers,
  });
}

export async function relayImagesBinding(
  request: Request,
  svc: ImagesFacade,
  policy: EgressPolicy | null,
): Promise<Response> {
  const url = new URL(request.url);
  const target = FACADE_BASE + url.pathname + url.search;
  const init: RequestInit = { method: request.method };
  const contentType = request.headers.get("content-type");
  if (contentType !== null) {
    init.headers = { "content-type": contentType };
  }
  let body: Uint8Array | undefined;
  if (request.method !== "GET" && request.method !== "HEAD") {
    body = new Uint8Array(await request.arrayBuffer());
    init.body = body;
  }

  if (body !== undefined && url.pathname === "/transform") {
    // latin1 keeps the scan byte-accurate; the markers name the
    // multipart fields StreamableFormData emits for text sources.
    const flat = new TextDecoder("latin1").decode(body);
    if (
      flat.includes('name="text_input"') ||
      flat.includes('name="draw_text"')
    ) {
      let form: FormData;
      try {
        form = await new Response(body, {
          headers: { "content-type": contentType ?? "" },
        }).formData();
      } catch {
        // Unparseable multipart — forward raw so the facade's own
        // validation produces the canonical rejection.
        return relayResponse(await svc.fetch(target, init));
      }
      const out = new FormData();
      for (const [name, value] of form) {
        out.append(name, value);
        if (
          (name === "text_input" || name === "draw_text") &&
          typeof value === "string"
        ) {
          const fontUrl = specFontUrl(value);
          if (fontUrl === undefined) continue;
          const font = await fetchFont(fontUrl, policy);
          if (font instanceof Response) return font;
          // The font file must follow its spec field — the facade
          // pairs them by multipart order (pendingText semantics).
          out.append(
            name === "text_input" ? "text_font" : "draw_font",
            new Blob([font as BlobPart]),
            "font",
          );
        }
      }
      return relayResponse(await svc.fetch(target, { method: "POST", body: out }));
    }
  }
  return relayResponse(await svc.fetch(target, init));
}
