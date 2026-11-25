// `cf.image` fetch-option handling inside the egress worker (IMG2).
//
// `fetch(url, {cf: {image: {...}}})` is Cloudflare's subrequest image
// transform: the tenant's `cf` blob rides the subrequest channel to
// this globalOutbound worker intact (`Request::serializeCfBlobJson`
// serializes the whole object), so `request.cf.image` arrives exactly
// as the tenant wrote it. The transform runs through the same
// facade-images multipart wire as the env.IMAGES binding — this module
// only translates the option vocabulary and re-shapes the response.
//
// Failure shape mirrors CF: transform errors surface as the facade's
// status with an `ERROR <code>` text body and a `cf-resized: err=<code>`
// header — never a silently untransformed image. Malformed option
// values reject the tenant's fetch() with TypeError, matching CF's
// invalid-option behavior.

const OPT_KEYS = new Set([
  "anim", "background", "blur", "border", "brightness", "compression",
  "contrast", "dpr", "draw", "fit", "flip", "format", "gamma", "gravity",
  "height", "image", "metadata", "onerror", "origin-auth", "padding",
  "quality", "rotate", "saturation", "segment", "sharpen",
  "slow-connection-quality", "trim", "width", "zoom",
]);

const SRC_MAX = 64 * 1024 * 1024;
/** `draw` entry cap mirrors the wire's MAX_DRAW_IMAGES. */
const MAX_DRAW = 16;

/** Draw-entry vocabulary: an image overlay (`url` + optimization
 * params + positioning/blending) or a text overlay (`text` + font —
 * unsupported without a vendored font stack). */
const DRAW_ENTRY_KEYS = new Set([
  "url", "text", "font", "color", "size",
  "top", "left", "bottom", "right", "opacity", "repeat", "composite",
  "width", "height", "fit", "gravity", "rotate", "blur", "background",
  "brightness", "contrast", "gamma", "saturation", "flip", "sharpen",
  "trim", "border", "padding", "zoom", "segment",
]);
const COMPOSITES = new Set(["over", "in", "atop", "out", "xor", "lighter"]);

/** One option object's transform vocabulary → wire op. Shared by the
 * top-level `cf.image` options and each `draw` entry's overlay params.
 * `percentDims` lets `width`/`height` take `"Np"` strings — draw
 * overlays resolve them against the base canvas via `percentBasis`,
 * not the overlay's own axis. */
function transformOp(
  src: Record<string, unknown>,
  dpr: number,
  percentDims: boolean,
): Record<string, unknown> {
  const num = (k: string, min: number, max: number): number | undefined => {
    const v = src[k];
    if (v === undefined) return undefined;
    if (typeof v !== "number" || !Number.isFinite(v) || v < min || v > max) {
      throw new TypeError(`cf.image: invalid ${k}`);
    }
    return v;
  };
  const op: Record<string, unknown> = {};
  const dim = (k: "width" | "height"): number | string | undefined => {
    const v = src[k];
    if (v === undefined) return undefined;
    // `"Np"` passes through for the facade to resolve against the
    // percentBasis frame (draw overlay dims are base-relative).
    if (percentDims && typeof v === "string" && v.endsWith("p")) {
      const n = Number(v.slice(0, -1));
      if (v.length < 2 || !Number.isFinite(n) || n <= 0) {
        throw new TypeError(`cf.image: invalid ${k}`);
      }
      return v;
    }
    if (typeof v !== "number" || !Number.isFinite(v) || v < 1 || v > 16384) {
      throw new TypeError(`cf.image: invalid ${k}`);
    }
    return Math.max(1, Math.round(v * dpr));
  };
  const w = dim("width");
  const h = dim("height");
  if (w !== undefined) op.width = w;
  if (h !== undefined) op.height = h;
  if (src.fit !== undefined) {
    // String mode, or the structured `{mode, upscale}` form — the
    // facade validates the mode list and fails `upscale:"generate"`
    // closed (no vendored ESRGAN).
    const f = src.fit;
    if (typeof f === "string") {
      op.fit = f;
    } else if (typeof f === "object" && f !== null && !Array.isArray(f)) {
      const fo = f as Record<string, unknown>;
      for (const k of Object.keys(fo)) {
        if (k !== "mode" && k !== "upscale") throw new TypeError(`cf.image: invalid fit.${k}`);
      }
      if (typeof fo.mode !== "string") throw new TypeError("cf.image: invalid fit.mode");
      op.fit = fo;
    } else {
      throw new TypeError("cf.image: invalid fit");
    }
  }
  if (src.flip !== undefined) {
    if (src.flip !== "h" && src.flip !== "v" && src.flip !== "hv") {
      throw new TypeError("cf.image: invalid flip");
    }
    op.flip = src.flip;
  }
  if (src.gravity !== undefined) {
    const g = src.gravity;
    if (typeof g === "string") {
      op.gravity = g;
    } else if (
      typeof g === "object" && g !== null &&
      typeof (g as Record<string, unknown>).x === "number" &&
      typeof (g as Record<string, unknown>).y === "number"
    ) {
      const gr = g as Record<string, unknown>;
      op.gravity = { x: gr.x, y: gr.y, mode: gr.mode ?? "remainder" };
    } else {
      throw new TypeError("cf.image: invalid gravity");
    }
  }
  if (src.rotate !== undefined) {
    const r = num("rotate", 0, 360) as number;
    if (![0, 90, 180, 270].includes(r)) throw new TypeError("cf.image: invalid rotate");
    op.rotate = r;
  }
  if (src.blur !== undefined) op.blur = num("blur", 1, 250);
  if (src.sharpen !== undefined) {
    // Scalar = box filter (legacy + `mode:"box"`); `mode:"gaussian"`
    // needs `sigma` (+ optional `threshold`) — 2026-11-19 changelog.
    const s = src.sharpen;
    if (typeof s === "number") {
      op.sharpen = num("sharpen", 0, 10);
    } else if (typeof s === "object" && s !== null && !Array.isArray(s)) {
      const so = s as Record<string, unknown>;
      for (const k of Object.keys(so)) {
        if (!["mode", "amount", "sigma", "threshold"].includes(k)) {
          throw new TypeError(`cf.image: invalid sharpen.${k}`);
        }
      }
      const mode = so.mode ?? "box";
      if (mode !== "box" && mode !== "gaussian") {
        throw new TypeError("cf.image: invalid sharpen.mode");
      }
      if (typeof so.amount !== "number" || !Number.isFinite(so.amount) || so.amount < 0 || so.amount > 10) {
        throw new TypeError("cf.image: invalid sharpen.amount");
      }
      const out: Record<string, unknown> = { mode, amount: so.amount };
      if (mode === "gaussian") {
        if (typeof so.sigma !== "number" || !Number.isFinite(so.sigma) || so.sigma <= 0 || so.sigma > 5) {
          throw new TypeError("cf.image: invalid sharpen.sigma");
        }
        out.sigma = so.sigma;
        if (so.threshold !== undefined) {
          if (typeof so.threshold !== "number" || !Number.isInteger(so.threshold) || so.threshold < 0 || so.threshold > 255) {
            throw new TypeError("cf.image: invalid sharpen.threshold");
          }
          out.threshold = so.threshold;
        }
      } else if (so.sigma !== undefined || so.threshold !== undefined) {
        throw new TypeError("cf.image: box sharpen takes no sigma/threshold");
      }
      op.sharpen = out;
    } else {
      throw new TypeError("cf.image: invalid sharpen");
    }
  }
  if (src.brightness !== undefined) op.brightness = num("brightness", 0, 3);
  if (src.contrast !== undefined) op.contrast = num("contrast", 0, 3);
  if (src.gamma !== undefined) op.gamma = num("gamma", 0.01, 10);
  if (src.saturation !== undefined) op.saturation = num("saturation", 0, 3);
  if (src.zoom !== undefined) op.zoom = num("zoom", 0.1, 8);
  if (src.background !== undefined) {
    if (typeof src.background !== "string" || src.background.length > 64) {
      throw new TypeError("cf.image: invalid background");
    }
    op.background = src.background;
  }
  if (src.segment !== undefined) op.segment = src.segment;
  if (src.trim !== undefined) {
    // Upstream forms: "color"/"border" auto border-trim, the
    // "top,right,bottom,left" inset string, or the structured object —
    // `{mode:"color", color?, tolerance?, keep?}` and
    // `{mode:"area"|implicit, top/right/bottom/left | left/top/width/height}`
    // where side values are px, `0<v<1` fractions, or `"Np"` percents.
    // Absolute pixel counts scale with dpr like width/height do.
    const t = src.trim;
    const trimNum = (v: unknown, key: string): unknown => {
      if (typeof v === "string" && v.endsWith("p")) {
        const n = Number(v.slice(0, -1));
        if (v.length > 1 && Number.isFinite(n) && n >= 0 && n <= 100) return v;
        throw new TypeError(`cf.image: invalid trim.${key}`);
      }
      if (typeof v === "string" && v !== "" && !Number.isNaN(Number(v))) v = Number(v);
      if (typeof v !== "number" || !Number.isFinite(v) || v < 0) {
        throw new TypeError(`cf.image: invalid trim.${key}`);
      }
      if (v > 0 && v < 1) return v; // axis fraction — resolved at the facade
      return Math.round(v * dpr);
    };
    if (t === "border" || t === "color") {
      op.trim = "border";
    } else if (typeof t === "string") {
      const parts = t.split(",").map((s) => Number(s));
      if (parts.length !== 4 || parts.some((n) => !Number.isFinite(n) || n < 0)) {
        throw new TypeError("cf.image: invalid trim");
      }
      const [top, right, bottom, left] = parts.map((n) => Math.round(n * dpr));
      op.trim = { top, right, bottom, left };
    } else if (typeof t === "object" && t !== null && !Array.isArray(t)) {
      const to = t as Record<string, unknown>;
      for (const k of Object.keys(to)) {
        if (!["mode", "top", "bottom", "left", "right", "width", "height", "color", "tolerance", "keep"].includes(k)) {
          throw new TypeError(`cf.image: invalid trim.${k}`);
        }
      }
      const colorKeys = ["color", "tolerance", "keep"];
      const hasColorKeys = colorKeys.some((k) => to[k] !== undefined);
      const areaKeys = ["top", "bottom", "left", "right", "width", "height"];
      const hasAreaKeys = areaKeys.some((k) => to[k] !== undefined);
      const mode = to.mode ?? (hasColorKeys && !hasAreaKeys ? "color" : "area");
      if (mode === "color") {
        if (hasAreaKeys) throw new TypeError("cf.image: trim color mode takes no area keys");
        const out: Record<string, unknown> = { mode: "color" };
        if (to.color !== undefined) {
          if (typeof to.color !== "string" || to.color.length > 64) {
            throw new TypeError("cf.image: invalid trim.color");
          }
          out.color = to.color;
        }
        if (to.tolerance !== undefined) {
          if (typeof to.tolerance !== "number" || !Number.isInteger(to.tolerance) || to.tolerance < 0 || to.tolerance > 255) {
            throw new TypeError("cf.image: invalid trim.tolerance");
          }
          out.tolerance = to.tolerance;
        }
        if (to.keep !== undefined) {
          if (typeof to.keep !== "number" || !Number.isInteger(to.keep) || to.keep < 0) {
            throw new TypeError("cf.image: invalid trim.keep");
          }
          out.keep = Math.round(to.keep * dpr);
        }
        op.trim = out;
      } else if (mode === "area") {
        if (hasColorKeys) throw new TypeError("cf.image: trim area mode takes no color keys");
        if ((to.width !== undefined || to.height !== undefined) && (to.right !== undefined || to.bottom !== undefined)) {
          throw new TypeError("cf.image: trim rect mixes side keys");
        }
        const out: Record<string, unknown> = { mode: "area" };
        for (const k of areaKeys) {
          if (to[k] !== undefined) out[k] = trimNum(to[k], k);
        }
        op.trim = out;
      } else {
        throw new TypeError("cf.image: invalid trim.mode");
      }
    } else {
      throw new TypeError("cf.image: invalid trim");
    }
  }
  for (const k of ["border", "padding"] as const) {
    if (src[k] !== undefined) {
      const v = src[k];
      // Border/padding pixel widths scale with dpr upstream. `padding`
      // rides the border op painted with the request's background
      // color (or fully transparent when none is set).
      const color = k === "padding"
        ? (typeof src.background === "string" ? src.background : "rgba(0,0,0,0)")
        : undefined;
      if (typeof v === "number") {
        if (!Number.isFinite(v) || v < 0) throw new TypeError(`cf.image: invalid ${k}`);
        op.border = { width: Math.round(v * dpr), ...(color !== undefined ? { color } : {}) };
      } else if (typeof v === "object" && v !== null && !Array.isArray(v)) {
        const bo = v as Record<string, unknown>;
        const out: Record<string, unknown> = {};
        for (const bk of Object.keys(bo)) {
          if (!["color", "width", "top", "bottom", "left", "right"].includes(bk)) {
            throw new TypeError(`cf.image: invalid ${k}.${bk}`);
          }
          if (bk === "color") {
            if (typeof bo.color !== "string" || bo.color.length > 64) {
              throw new TypeError(`cf.image: invalid ${k}.color`);
            }
            out.color = bo.color;
          } else {
            const n = bo[bk];
            if (typeof n !== "number" || !Number.isFinite(n) || n < 0) {
              throw new TypeError(`cf.image: invalid ${k}.${bk}`);
            }
            out[bk] = Math.round(n * dpr);
          }
        }
        if (color !== undefined && out.color === undefined) out.color = color;
        op.border = out;
      } else {
        throw new TypeError(`cf.image: invalid ${k}`);
      }
    }
  }
  return op;
}

export interface CfImagePlan {
  transforms: string;
  /** Overlay URLs to fetch, in draw order — each lands as a
   * `draw_image` multipart field (`frames[1..]` on the wire). */
  draws: string[];
  outputFormat?: string;
  outputQuality?: number;
  anim: boolean;
  metadata: string | null;
  json: boolean;
  onerrorRedirect: boolean;
}

/** Extract + validate `request.cf.image`. `undefined` = no transform. */
export function cfImagePlan(request: Request): CfImagePlan | undefined {
  const cf = (request as unknown as { cf?: { image?: unknown } }).cf;
  if (cf === undefined || cf === null || typeof cf !== "object") return undefined;
  const image = cf.image;
  if (image === undefined || image === null) return undefined;
  if (typeof image !== "object") {
    throw new TypeError("cf.image must be an object");
  }
  const o = image as Record<string, unknown>;
  for (const k of Object.keys(o)) {
    if (!OPT_KEYS.has(k)) throw new TypeError(`cf.image: unknown option ${k}`);
  }
  // Keys with no engine support fail closed through the facade wire.
  for (const k of ["image", "origin-auth", "slow-connection-quality"]) {
    if (o[k] !== undefined) {
      throw new TypeError(`cf.image: ${k} is not supported`);
    }
  }
  const num = (k: string, min: number, max: number): number | undefined => {
    const v = o[k];
    if (v === undefined) return undefined;
    if (typeof v !== "number" || !Number.isFinite(v) || v < min || v > max) {
      throw new TypeError(`cf.image: invalid ${k}`);
    }
    return v;
  };
  // Upstream caps dpr at 2.
  const dpr = num("dpr", 0.1, 2) ?? 1;
  const op = transformOp(o, dpr, false);
  if (o.compression !== undefined && o.compression !== "fast") {
    throw new TypeError("cf.image: invalid compression");
  }
  let outputFormat: string | undefined;
  let json = false;
  if (o.format !== undefined) {
    switch (o.format) {
      case "avif": outputFormat = "image/avif"; break;
      case "webp": outputFormat = "image/webp"; break;
      case "jpeg":
      case "baseline-jpeg": outputFormat = "image/jpeg"; break;
      case "png":
      case "png-force": outputFormat = "image/png"; break;
      case "json": json = true; break;
      case "gif":
      case "rgb":
      case "rgba": outputFormat = `image/${o.format}`; break;
      default: throw new TypeError("cf.image: invalid format");
    }
  }
  // `compression=fast` picks the quickest format upstream — JPEG —
  // overriding an explicit `format`.
  if (o.compression === "fast") outputFormat = "image/jpeg";
  let outputQuality: number | undefined;
  if (o.quality !== undefined) {
    if (typeof o.quality === "string") {
      // Perceptual tiers — upstream publishes no integer mapping;
      // anchors match other CF-compat implementations.
      const TIER: Record<string, number> = {
        high: 90, "medium-high": 80, "medium-low": 60, low: 40,
      };
      const q = TIER[o.quality];
      if (q === undefined) throw new TypeError("cf.image: invalid quality");
      outputQuality = q;
    } else {
      outputQuality = num("quality", 1, 100);
    }
  }
  const anim = o.anim === true;
  // Absent → null (facade defaults to `copyright`, matching upstream);
  // `none` must reach the wire explicitly or it would be ignored.
  const metadata =
    o.metadata === undefined ? null
    : o.metadata === "none" || o.metadata === "keep" || o.metadata === "copyright" ? o.metadata as string
    : (() => { throw new TypeError("cf.image: invalid metadata"); })();
  // `draw` overlays ride the wire as draw_image attachments +
  // per-entry ops: `[mainOp, overOp1, drawOp1, ...]` — the ops apply
  // in order so the last entry composites topmost.
  const draws: string[] = [];
  const ops: Record<string, unknown>[] = [op];
  if (o.draw !== undefined) {
    if (!Array.isArray(o.draw)) throw new TypeError("cf.image: draw must be an array");
    if (o.draw.length > MAX_DRAW) throw new TypeError("cf.image: too many draw entries");
    for (let i = 0; i < o.draw.length; i++) {
      const e = o.draw[i];
      if (typeof e !== "object" || e === null || Array.isArray(e)) {
        throw new TypeError(`cf.image: invalid draw entry`);
      }
      for (const k of Object.keys(e as Record<string, unknown>)) {
        if (!DRAW_ENTRY_KEYS.has(k)) {
          throw new TypeError(`cf.image: unknown draw option ${k}`);
        }
      }
      const entry = e as Record<string, unknown>;
      // Text overlays need a harfbuzz/freetype stack — not vendored.
      if (entry.text !== undefined) {
        throw new TypeError("cf.image: draw text is not supported");
      }
      for (const k of ["font", "color", "size"]) {
        if (entry[k] !== undefined) {
          throw new TypeError(`cf.image: draw.${k} applies to text overlays`);
        }
      }
      if (typeof entry.url !== "string" || entry.url === "") {
        throw new TypeError("cf.image: invalid draw.url");
      }
      let proto: string;
      try {
        proto = new URL(entry.url).protocol;
      } catch {
        throw new TypeError("cf.image: invalid draw.url");
      }
      if (proto !== "http:" && proto !== "https:") {
        throw new TypeError("cf.image: invalid draw.url");
      }
      const drawOp: Record<string, unknown> = {
        drawImageIndex: i + 1,
        targetImageIndex: 0,
      };
      for (const k of ["top", "left", "bottom", "right"] as const) {
        const v = entry[k];
        if (v === undefined) continue;
        if (typeof v !== "number" || !Number.isFinite(v)) {
          throw new TypeError(`cf.image: invalid draw.${k}`);
        }
        drawOp[k] = Math.round(v * dpr);
      }
      // Upstream errors when an axis carries both edge offsets.
      if (entry.left !== undefined && entry.right !== undefined) {
        throw new TypeError("cf.image: draw left+right conflict");
      }
      if (entry.top !== undefined && entry.bottom !== undefined) {
        throw new TypeError("cf.image: draw top+bottom conflict");
      }
      if (entry.opacity !== undefined) {
        const v = entry.opacity;
        if (typeof v !== "number" || !Number.isFinite(v) || v < 0 || v > 1) {
          throw new TypeError("cf.image: invalid draw.opacity");
        }
        drawOp.opacity = v;
      }
      if (entry.repeat !== undefined) {
        const v = entry.repeat;
        if (v !== true && v !== "x" && v !== "y") {
          throw new TypeError("cf.image: invalid draw.repeat");
        }
        drawOp.repeat = v;
      }
      if (entry.composite !== undefined) {
        const v = entry.composite;
        if (typeof v !== "string" || !COMPOSITES.has(v)) {
          throw new TypeError("cf.image: invalid draw.composite");
        }
        drawOp.composite = v;
      }
      // Overlay optimization params share the top-level vocabulary;
      // percentBasis marks `"Np"` dims for base-canvas resolution.
      const overOp = transformOp(entry, dpr, true);
      if (Object.keys(overOp).length > 0) {
        overOp.imageIndex = i + 1;
        overOp.percentBasis = 0;
        ops.push(overOp);
      }
      ops.push(drawOp);
      draws.push(entry.url);
    }
  }
  return {
    transforms: JSON.stringify(ops),
    draws,
    outputFormat,
    outputQuality,
    anim,
    metadata,
    json,
    onerrorRedirect: o.onerror === "redirect",
  };
}

/** Multipart body for the facade-images `/transform` wire. `overlays`
 * are the fetched draw-entry images, appended in plan.draws order —
 * each becomes `frames[i+1]` for the per-entry draw ops. */
export function cfImageBody(
  src: Uint8Array,
  plan: CfImagePlan,
  overlays: Uint8Array[] = [],
): FormData {
  const fd = new FormData();
  fd.append("image", new Blob([src as BlobPart]), "source");
  fd.append("transforms", plan.transforms);
  // output_format is required by the wire; `format:"json"` still encodes
  // internally (jpeg) — the JSON body reports the result's sniffed meta.
  fd.append("output_format", plan.outputFormat ?? "image/jpeg");
  if (plan.outputQuality !== undefined) fd.append("output_quality", String(plan.outputQuality));
  if (plan.anim) fd.append("anim", "true");
  if (plan.metadata !== null) fd.append("metadata", plan.metadata);
  for (const over of overlays) {
    fd.append("draw_image", new Blob([over as BlobPart]), "overlay");
  }
  return fd;
}

export { SRC_MAX };
