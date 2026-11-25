// facade-images transform-chain executor — parses the upstream shim's
// `transforms` JSON into calls on images-engine. Op vocabulary mirrors
// `ImageTransform`/`ImageDrawOptions` in workerd's images.d.ts; anything
// outside that vocabulary fails closed rather than being silently
// dropped.
//
// Order inside one ImageTransform object (the order CF applies them):
//   trim → gravity crop → fit resize/pad → rotate → flip → color ops →
//   blur → sharpen → border paint
import {
  EngineError,
  adjustImage,
  blurImage,
  cropImage,
  drawImage,
  flipImage,
  padImage,
  parseColor,
  resizeExact,
  rotateImage,
  sharpenImage,
  gaussianSharpenImage,
  type ImageDataLike,
} from "./images-engine.js";

/** Decode-path pixel bound — decoded RGBA needs headroom inside the
 * worker memory limit (70MP would be ~280MB). */
export const IMAGES_TRANSFORM_MAX_PIXELS = 16_000_000;
/** Overlay count bound. */
const MAX_DRAW_IMAGES = 16;

const TRANSFORM_KEYS = new Set([
  "imageIndex",
  "width",
  "height",
  "background",
  "blur",
  "border",
  "brightness",
  "contrast",
  "fit",
  "flip",
  "gamma",
  "gravity",
  "rotate",
  "saturation",
  "segment",
  "sharpen",
  "trim",
  "zoom",
]);

const DRAW_KEYS = new Set([
  "drawImageIndex",
  "targetImageIndex",
  "opacity",
  "repeat",
  "composite",
  "top",
  "left",
  "bottom",
  "right",
]);

const FITS = new Set(["scale-down", "contain", "pad", "squeeze", "cover", "crop", "aspect-crop", "scale-up"]);
const COMPOSITES = new Set(["over", "in", "atop", "out", "xor", "lighter"]);

function bad(msg: string): never {
  throw new EngineError(9423, `images: ${msg}`);
}
function unsupported(msg: string): never {
  throw new EngineError(9401, `images: ${msg}`);
}

function num(v: unknown, key: string, { min = 0, max = Number.MAX_SAFE_INTEGER, int = false } = {}): number | undefined {
  if (v === undefined) return undefined;
  if (typeof v !== "number" || !Number.isFinite(v) || v < min || v > max || (int && !Number.isInteger(v))) {
    bad(`invalid ${key}`);
  }
  return v;
}

/** Anchor point (0..1 fractions) from a gravity value. */
function gravityAnchor(gravity: unknown): { ax: number; ay: number; box: boolean; entropy: boolean } {
  if (gravity === undefined) return { ax: 0.5, ay: 0.5, box: false, entropy: false };
  switch (gravity) {
    case "center": return { ax: 0.5, ay: 0.5, box: false, entropy: false };
    case "left": return { ax: 0, ay: 0.5, box: false, entropy: false };
    case "right": return { ax: 1, ay: 0.5, box: false, entropy: false };
    case "top": return { ax: 0.5, ay: 0, box: false, entropy: false };
    case "bottom": return { ax: 0.5, ay: 1, box: false, entropy: false };
    case "entropy":
    case "auto":
      // `auto` is saliency-driven upstream; the honest local
      // approximation is the entropy crop.
      return { ax: 0.5, ay: 0.5, box: false, entropy: true };
    case "face":
      // No face model is vendored — fall back to the entropy heuristic
      // rather than pretending (documented divergence).
      return { ax: 0.5, ay: 0.5, box: false, entropy: true };
    default:
      if (typeof gravity === "object" && gravity !== null) {
        const g = gravity as Record<string, unknown>;
        const keys = Object.keys(g);
        if (!keys.every((k) => k === "x" || k === "y" || k === "mode")) bad("gravity object keys");
        if (g.mode !== "remainder" && g.mode !== "box-center") bad("gravity.mode");
        const x = num(g.x, "gravity.x", { min: 0, max: 1 }) ?? 0.5;
        const y = num(g.y, "gravity.y", { min: 0, max: 1 }) ?? 0.5;
        return { ax: x, ay: y, box: g.mode === "box-center", entropy: false };
      }
      bad("invalid gravity");
  }
}

/** Entropy crop: pick the candidate crop window with the highest luma
 * variance over a coarse grid — the local stand-in for CF's saliency
 * model. */
function entropyCrop(img: ImageDataLike, w: number, h: number): { x: number; y: number } {
  const maxX = img.width - w;
  const maxY = img.height - h;
  if (maxX <= 0 && maxY <= 0) return { x: 0, y: 0 };
  const steps = 8;
  let best = { x: Math.floor(maxX / 2), y: Math.floor(maxY / 2) };
  let bestVar = -1;
  for (let gy = 0; gy <= steps; gy++) {
    for (let gx = 0; gx <= steps; gx++) {
      const cx = Math.floor((maxX * gx) / steps);
      const cy = Math.floor((maxY * gy) / steps);
      let mean = 0;
      let sq = 0;
      let n = 0;
      const stepY = Math.max(1, Math.floor(h / 64));
      const stepX = Math.max(1, Math.floor(w / 64));
      for (let py = 0; py < h; py += stepY) {
        for (let px = 0; px < w; px += stepX) {
          const i = ((cy + py) * img.width + cx + px) * 4;
          const luma = 0.2126 * img.data[i] + 0.7152 * img.data[i + 1] + 0.0722 * img.data[i + 2];
          mean += luma;
          sq += luma * luma;
          n++;
        }
      }
      if (!n) continue;
      const variance = sq / n - (mean / n) ** 2;
      if (variance > bestVar) {
        bestVar = variance;
        best = { x: cx, y: cy };
      }
    }
  }
  return best;
}

/** Crop-rect for a target aspect/size at the given gravity anchor. */
function cropRectFor(
  img: ImageDataLike,
  w: number,
  h: number,
  anchor: { ax: number; ay: number; box: boolean; entropy: boolean },
): { x: number; y: number; w: number; h: number } {
  w = Math.min(img.width, Math.max(1, Math.round(w)));
  h = Math.min(img.height, Math.max(1, Math.round(h)));
  if (anchor.entropy) {
    const { x, y } = entropyCrop(img, w, h);
    return { x, y, w, h };
  }
  if (anchor.box) {
    // box-center: crop centered on the focal point.
    const x = Math.round(anchor.ax * img.width - w / 2);
    const y = Math.round(anchor.ay * img.height - h / 2);
    return {
      x: Math.max(0, Math.min(img.width - w, x)),
      y: Math.max(0, Math.min(img.height - h, y)),
      w,
      h,
    };
  }
  // side/center/remainder: anchor fraction positions the crop window.
  return {
    x: Math.round(anchor.ax * (img.width - w)),
    y: Math.round(anchor.ay * (img.height - h)),
    w,
    h,
  };
}

/** Auto-trim of a near-uniform border. `tolerance` is a slack value —
 * pixels within `dist <= tolerance` (per-channel euclidean) count as
 * border. With no `refColor` the top-left pixel is the detected border
 * color, the local stand-in for upstream's auto-detection. Keeps
 * `keep` pixels of margin. */
function trimBorder(
  img: ImageDataLike,
  tolerance: number,
  keep: number,
  refColor?: [number, number, number],
): ImageDataLike {
  const { width: w, height: h, data } = img;
  const tol2 = tolerance * tolerance * 3;
  const ref = refColor ?? [data[0], data[1], data[2]];
  const near = (i: number) => {
    const dr = data[i] - ref[0];
    const dg = data[i + 1] - ref[1];
    const db = data[i + 2] - ref[2];
    return dr * dr + dg * dg + db * db <= tol2;
  };
  let top = 0;
  outerTop: while (top < h) {
    for (let x = 0; x < w; x++) if (!near((top * w + x) * 4)) break outerTop;
    top++;
  }
  let bottom = h;
  outerBottom: while (bottom > top) {
    for (let x = 0; x < w; x++) if (!near(((bottom - 1) * w + x) * 4)) break outerBottom;
    bottom--;
  }
  let left = 0;
  outerLeft: while (left < w) {
    for (let y = top; y < bottom; y++) if (!near((y * w + left) * 4)) break outerLeft;
    left++;
  }
  let right = w;
  outerRight: while (right > left) {
    for (let y = top; y < bottom; y++) if (!near((y * w + right - 1) * 4)) break outerRight;
    right--;
  }
  const x = Math.max(0, left - keep);
  const y = Math.max(0, top - keep);
  const nw = Math.min(w, right + keep) - x;
  const nh = Math.min(h, bottom + keep) - y;
  if (nw <= 0 || nh <= 0 || (x === 0 && y === 0 && nw === w && nh === h)) return img;
  return cropImage(img, x, y, nw, nh);
}

/** A trim length: integer pixel count, a `0 < v < 1` fraction of the
 * axis, or a `"Np"` percentage string (upstream's three forms). */
function trimLength(v: unknown, total: number, key: string): number {
  if (typeof v === "string" && v.endsWith("p")) {
    const n = Number(v.slice(0, -1));
    if (Number.isFinite(n) && n >= 0 && n <= 100 && v.length > 1) {
      return Math.round((n / 100) * total);
    }
  }
  if (typeof v === "string" && v !== "" && !Number.isNaN(Number(v))) {
    v = Number(v); // numeric strings ride the same rules as numbers
  }
  if (typeof v !== "number" || !Number.isFinite(v) || v < 0) bad(key);
  const n = v as number;
  if (n > 0 && n < 1) return Math.round(n * total);
  return Math.min(Math.round(n), total);
}

function trimColorMode(img: ImageDataLike, t: Record<string, unknown>): ImageDataLike {
  const ref = t.color === undefined
    ? undefined
    : (typeof t.color === "string" ? parseColor(t.color) : null) ?? bad("trim.color");
  // Upstream documents tolerance as "how closely pixels must match"
  // on a 0..255 scale; the slack reading (0 = exact match) is what
  // the documented examples mean.
  const tolerance = num(t.tolerance, "trim.tolerance", { min: 0, max: 255, int: true }) ?? 16;
  const keep = num(t.keep, "trim.keep", { min: 0, max: 4096, int: true }) ?? 0;
  return trimBorder(img, tolerance, keep, ref === undefined ? undefined : [ref[0], ref[1], ref[2]]);
}

function applyTrim(img: ImageDataLike, trim: unknown): ImageDataLike {
  if (trim === undefined) return img;
  // `border` is the pre-rename upstream string; `color` is the current one.
  if (trim === "border" || trim === "color") return trimBorder(img, 16, 0);
  if (typeof trim !== "object" || trim === null) bad("trim");
  const t = trim as Record<string, unknown>;
  for (const k of Object.keys(t)) {
    if (!["mode", "top", "bottom", "left", "right", "width", "height", "border", "color", "tolerance", "keep"].includes(k)) {
      bad(`trim.${k}`);
    }
  }
  const mode = t.mode;
  if (mode === "color") {
    for (const k of Object.keys(t)) {
      if (!["mode", "color", "tolerance", "keep"].includes(k)) bad(`trim.${k}`);
    }
    return trimColorMode(img, t);
  }
  // Legacy internal form `{border:{color,tolerance,keep}}` — the
  // pre-rename upstream binding shape; still accepted on the wire.
  if (t.border !== undefined && mode === undefined) {
    const b = t.border === true ? {} : t.border;
    if (typeof b !== "object" || b === null) bad("trim.border");
    const bo = b as Record<string, unknown>;
    for (const k of Object.keys(bo)) {
      if (!["color", "tolerance", "keep"].includes(k)) bad(`trim.border.${k}`);
    }
    return trimColorMode(img, bo);
  }
  // Area mode: explicit `mode:"area"` or inferred from bare side/rect
  // keys (upstream infers `trim={left:800}` as area).
  if (mode !== undefined && mode !== "area") bad("trim.mode");
  for (const k of Object.keys(t)) {
    if (!["mode", "top", "bottom", "left", "right", "width", "height"].includes(k)) bad(`trim.${k}`);
  }
  if (t.width !== undefined || t.height !== undefined) {
    // Preserve-rectangle form: left/top position the window, width/
    // height size it (both required to pin the rect).
    if (t.right !== undefined || t.bottom !== undefined) bad("trim rect mixes side keys");
    const x = t.left === undefined ? 0 : trimLength(t.left, img.width, "trim.left");
    const y = t.top === undefined ? 0 : trimLength(t.top, img.height, "trim.top");
    const w = trimLength(t.width, img.width, "trim.width");
    const h = trimLength(t.height, img.height, "trim.height");
    if (w < 1 || h < 1) bad("trim rect is empty");
    if (x + w > img.width || y + h > img.height) bad("trim rect exceeds the image");
    return cropImage(img, x, y, w, h);
  }
  const top = t.top === undefined ? 0 : trimLength(t.top, img.height, "trim.top");
  const bottom = t.bottom === undefined ? 0 : trimLength(t.bottom, img.height, "trim.bottom");
  const left = t.left === undefined ? 0 : trimLength(t.left, img.width, "trim.left");
  const right = t.right === undefined ? 0 : trimLength(t.right, img.width, "trim.right");
  const w = img.width - left - right;
  const h = img.height - top - bottom;
  if (w < 1 || h < 1) bad("trim removes the whole image");
  return cropImage(img, left, top, w, h);
}

/** A border width: integer pixel count or a `"Np"` percent of the
 * side's axis (`total`). Unlike trim lengths, pixel counts are not
 * capped by the axis — a border adds area beyond the image. */
function borderLen(v: unknown, total: number, key: string): number | undefined {
  if (v === undefined) return undefined;
  if (typeof v === "string" && v.endsWith("p")) {
    const n = Number(v.slice(0, -1));
    if (Number.isFinite(n) && n >= 0 && n <= 100 && v.length > 1) {
      return Math.round((n / 100) * total);
    }
  }
  return num(v, key, { min: 0, max: 4096 });
}

/** `sharpen` — scalar means the legacy box filter; the structured
 * form picks `box` or `gaussian` by `mode` (upstream 2026-11-19
 * "Gaussian sharpening"). `amount` is always required in the object
 * form; `sigma`/`threshold` belong to gaussian only. */
function applySharpen(img: ImageDataLike, sharpen: unknown): ImageDataLike {
  if (typeof sharpen === "number") {
    return sharpenImage(img, num(sharpen, "sharpen", { min: 0, max: 10 })!);
  }
  if (typeof sharpen !== "object" || sharpen === null || Array.isArray(sharpen)) bad("sharpen");
  const s = sharpen as Record<string, unknown>;
  for (const k of Object.keys(s)) {
    if (!["mode", "amount", "sigma", "threshold"].includes(k)) bad(`sharpen.${k}`);
  }
  const mode = s.mode ?? "box";
  if (mode !== "box" && mode !== "gaussian") bad("sharpen.mode");
  const amount = num(s.amount, "sharpen.amount", { min: 0, max: 10 });
  if (amount === undefined) bad("sharpen.amount");
  if (mode === "box") {
    if (s.sigma !== undefined || s.threshold !== undefined) bad("sharpen: box takes no sigma/threshold");
    return sharpenImage(img, amount);
  }
  const sigma = num(s.sigma, "sharpen.sigma", { min: Number.MIN_VALUE, max: 5 });
  if (sigma === undefined || sigma <= 0) bad("sharpen.sigma");
  const threshold = num(s.threshold, "sharpen.threshold", { min: 0, max: 255, int: true }) ?? 0;
  return gaussianSharpenImage(img, amount, sigma, threshold);
}

/** One `{imageIndex, ...ImageTransform}` op against a frame. */
async function applyTransform(img: ImageDataLike, op: Record<string, unknown>): Promise<ImageDataLike> {
  for (const k of Object.keys(op)) {
    if (!TRANSFORM_KEYS.has(k)) bad(`unknown transform key ${JSON.stringify(k)}`);
  }
  if (op.segment !== undefined) unsupported("segment requires an ML model the vendored engine lacks");
  if (op.zoom !== undefined && op.zoom !== 1) unsupported("zoom is not implemented");
  if (op.rotate !== undefined && ![0, 90, 180, 270].includes(op.rotate as number)) bad("rotate");

  // 1. trim
  img = applyTrim(img, op.trim);

  // 2. geometry: gravity crop + fit resize (+ pad letterbox)
  const tw = num(op.width, "width", { min: 1, max: 16384 });
  const th = num(op.height, "height", { min: 1, max: 16384 });
  const fitSrc = op.fit === undefined ? "scale-down" : op.fit;
  let fit: string;
  if (typeof fitSrc === "string") {
    fit = fitSrc;
  } else if (typeof fitSrc === "object" && fitSrc !== null && !Array.isArray(fitSrc)) {
    // Structured form `{mode, upscale}` (2026-11-18 "consistent
    // language"). `upscale: "generate"` is AI upscaling — no model is
    // vendored, so it fails closed; "interpolate" is our path anyway.
    const f = fitSrc as Record<string, unknown>;
    for (const k of Object.keys(f)) {
      if (k !== "mode" && k !== "upscale") bad(`fit.${k}`);
    }
    if (f.upscale === "generate") unsupported("upscale=generate needs the ESRGAN model, not vendored");
    if (f.upscale !== undefined && f.upscale !== "interpolate") bad("fit.upscale");
    if (typeof f.mode !== "string") bad("fit.mode");
    fit = f.mode;
  } else {
    bad("fit");
  }
  if (!FITS.has(fit)) bad("fit");
  const anchor = gravityAnchor(op.gravity);
  const bgColor = op.background !== undefined
    ? (typeof op.background === "string" ? parseColor(op.background) : null) ?? bad("background color")
    : ([0, 0, 0, 0] as [number, number, number, number]);

  if (tw !== undefined || th !== undefined) {
    const sw = img.width;
    const sh = img.height;
    const w = tw ?? Math.max(1, Math.round((th! / sh) * sw));
    const h = th ?? Math.max(1, Math.round((tw! / sw) * sh));
    switch (fit) {
      case "crop": {
        const r = cropRectFor(img, w, h, anchor);
        img = cropImage(img, r.x, r.y, r.w, r.h);
        break;
      }
      case "cover": {
        // Crop the largest aspect-matched window at the anchor, then resize.
        const scale = Math.max(w / sw, h / sh);
        const cw = Math.min(sw, Math.round(w / scale));
        const ch = Math.min(sh, Math.round(h / scale));
        const r = cropRectFor(img, cw, ch, anchor);
        img = await resizeExact(cropImage(img, r.x, r.y, r.w, r.h), w, h);
        break;
      }
      case "pad": {
        const scale = Math.min(w / sw, h / sh);
        const rw = Math.max(1, Math.round(sw * scale));
        const rh = Math.max(1, Math.round(sh * scale));
        img = await resizeExact(img, rw, rh);
        const l = Math.floor((w - rw) * anchor.ax);
        const t = Math.floor((h - rh) * anchor.ay);
        img = padImage(img, l, t, w - rw - l, h - rh - t, bgColor);
        break;
      }
      case "squeeze":
        img = await resizeExact(img, w, h);
        break;
      case "contain": {
        const scale = Math.min(w / sw, h / sh);
        img = await resizeExact(img, Math.max(1, Math.round(sw * scale)), Math.max(1, Math.round(sh * scale)));
        break;
      }
      case "scale-down": {
        const scale = Math.min(1, Math.min(w / sw, h / sh));
        img = await resizeExact(img, Math.max(1, Math.round(sw * scale)), Math.max(1, Math.round(sh * scale)));
        break;
      }
      case "scale-up": {
        // Contain, but only when the source is smaller — it never
        // downscales (a larger source is returned at its own size).
        const scale = Math.min(w / sw, h / sh);
        if (scale > 1) {
          img = await resizeExact(img, Math.max(1, Math.round(sw * scale)), Math.max(1, Math.round(sh * scale)));
        }
        break;
      }
      case "aspect-crop": {
        // Match the target aspect without upscaling: downscale just
        // enough to still fill the target, then crop to w:h at the
        // anchor. The output keeps the cropped source dims — it is
        // NOT resized to the requested box (upstream semantics).
        const scale = Math.min(1, Math.max(w / sw, h / sh));
        if (scale < 1) {
          img = await resizeExact(img, Math.max(1, Math.round(sw * scale)), Math.max(1, Math.round(sh * scale)));
        }
        const aspect = w / h;
        let cw = img.width;
        let ch = Math.round(img.width / aspect);
        if (ch > img.height) {
          ch = img.height;
          cw = Math.round(img.height * aspect);
        }
        const r = cropRectFor(img, cw, ch, anchor);
        img = cropImage(img, r.x, r.y, r.w, r.h);
        break;
      }
    }
  } else if (op.gravity !== undefined || fit === "crop") {
    // Gravity without dims only matters under crop semantics.
    if (fit === "crop") {
      const r = cropRectFor(img, img.width, img.height, anchor);
      img = cropImage(img, r.x, r.y, r.w, r.h);
    }
  }

  // 3. rotate / flip
  if (op.rotate === 90 || op.rotate === 180 || op.rotate === 270) {
    img = rotateImage(img, op.rotate as 90 | 180 | 270);
  }
  if (op.flip !== undefined) {
    if (op.flip !== "h" && op.flip !== "v" && op.flip !== "hv") bad("flip");
    img = flipImage(img, op.flip);
  }

  // 4. color ops
  if (op.brightness !== undefined || op.contrast !== undefined || op.gamma !== undefined || op.saturation !== undefined) {
    img = adjustImage(img, {
      brightness: num(op.brightness, "brightness", { min: 0, max: 3 }),
      contrast: num(op.contrast, "contrast", { min: 0, max: 3 }),
      gamma: num(op.gamma, "gamma", { min: 0.01, max: 10 }),
      saturation: num(op.saturation, "saturation", { min: 0, max: 3 }),
    });
  }

  // 5. blur / sharpen
  if (op.blur !== undefined) img = blurImage(img, num(op.blur, "blur", { min: 1, max: 250 })!);
  if (op.sharpen !== undefined) img = applySharpen(img, op.sharpen);

  // 6. border paint (after resize/effects, CF semantics)
  if (op.border !== undefined) {
    const b = op.border as Record<string, unknown> | null;
    if (typeof b !== "object" || b === null) bad("border");
    for (const k of Object.keys(b)) {
      if (!["color", "width", "top", "bottom", "left", "right"].includes(k)) bad(`border.${k}`);
    }
    const color = b.color !== undefined
      ? (typeof b.color === "string" ? parseColor(b.color) : null) ?? bad("border.color")
      : ([0, 0, 0, 255] as [number, number, number, number]);
    // Widths are px ints or `"Np"` percents resolved against each
    // side's own axis (upstream `border=(width=10p)`); `width` is the
    // uniform fallback resolved per-side.
    const l = borderLen(b.left ?? b.width, img.width, "border.left") ?? 0;
    const t = borderLen(b.top ?? b.width, img.height, "border.top") ?? 0;
    const r = borderLen(b.right ?? b.width, img.width, "border.right") ?? 0;
    const bo = borderLen(b.bottom ?? b.width, img.height, "border.bottom") ?? 0;
    img = padImage(img, l, t, r, bo, color);
  }

  if (img.width * img.height > IMAGES_TRANSFORM_MAX_PIXELS) {
    throw new EngineError(9401, "images: transform result exceeds the pixel cap");
  }
  return img;
}

/** `{drawImageIndex, targetImageIndex, ...DrawOptions}` op. */
function applyDraw(frames: ImageDataLike[], op: Record<string, unknown>): void {
  for (const k of Object.keys(op)) {
    if (!DRAW_KEYS.has(k)) bad(`unknown draw key ${JSON.stringify(k)}`);
  }
  const drawIdx = num(op.drawImageIndex, "drawImageIndex", { min: 1, int: true });
  const targetIdx = num(op.targetImageIndex, "targetImageIndex", { min: 0, int: true });
  if (drawIdx === undefined || targetIdx === undefined) bad("draw indices");
  if (drawIdx >= frames.length || targetIdx >= frames.length) bad("draw index out of range");
  // Upstream errors when an axis carries both edge offsets.
  if (op.left !== undefined && op.right !== undefined) bad("draw left+right");
  if (op.top !== undefined && op.bottom !== undefined) bad("draw top+bottom");
  const mode = op.composite ?? "over";
  if (typeof mode !== "string" || !COMPOSITES.has(mode)) bad("composite");
  const opacity = num(op.opacity, "opacity", { min: 0, max: 1 }) ?? 1;
  if (op.repeat !== undefined && typeof op.repeat !== "boolean" && typeof op.repeat !== "string") bad("repeat");
  frames[targetIdx] = drawImage(frames[targetIdx], frames[drawIdx], {
    top: num(op.top, "top"),
    left: num(op.left, "left"),
    bottom: num(op.bottom, "bottom"),
    right: num(op.right, "right"),
    opacity,
    repeat: op.repeat as boolean | string | undefined,
    composite: mode as "over",
  });
}

/** Run the upstream `transforms` array against decoded frames.
 * `frames[0]` is the canvas; indices ≥1 are `draw_image` attachments in
 * order. Returns the finished canvas. */
export async function applyTransforms(frames: ImageDataLike[], ops: unknown[]): Promise<ImageDataLike> {
  if (frames.length === 0 || !(frames[0].data instanceof Uint8ClampedArray)) {
    bad("missing canvas image");
  }
  if (frames.length - 1 > MAX_DRAW_IMAGES) bad("too many draw images");
  for (const frame of frames) {
    if (frame.width * frame.height > IMAGES_TRANSFORM_MAX_PIXELS) {
      throw new EngineError(9401, "images: source exceeds the transform pixel cap");
    }
  }
  for (const rawOp of ops) {
    if (typeof rawOp !== "object" || rawOp === null || Array.isArray(rawOp)) {
      bad("transform op must be an object");
    }
    const op = rawOp as Record<string, unknown>;
    if ("drawImageIndex" in op || "targetImageIndex" in op) {
      applyDraw(frames, op);
    } else {
      const idx = num(op.imageIndex, "imageIndex", { min: 0, int: true }) ?? 0;
      if (idx >= frames.length) bad("imageIndex out of range");
      // Draw-entry `"Np"` dims are relative to the BASE canvas
      // (upstream `width:"25p"` = 25% of the base axis), not the
      // overlay's own axis like trim percents — resolve them against
      // frames[percentBasis] before the op runs.
      if (op.percentBasis !== undefined) {
        const basis = num(op.percentBasis, "percentBasis", { min: 0, int: true });
        if (basis === undefined || basis >= frames.length) bad("percentBasis out of range");
        for (const dim of ["width", "height"] as const) {
          const v = op[dim];
          if (typeof v === "string" && v.endsWith("p")) {
            const n = Number(v.slice(0, -1));
            if (!Number.isFinite(n) || n <= 0 || v.length < 2) bad(`draw ${dim} percent`);
            op[dim] = Math.max(1, Math.round((n / 100) * frames[basis][dim]));
          }
        }
        delete op.percentBasis;
      }
      frames[idx] = await applyTransform(frames[idx], op);
    }
  }
  return frames[0];
}
