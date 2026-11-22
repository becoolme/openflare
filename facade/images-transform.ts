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

const FITS = new Set(["scale-down", "contain", "pad", "squeeze", "cover", "crop"]);
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

/** Auto-trim of a near-uniform border within `tolerance`; keeps `keep`
 * pixels of margin. */
function trimBorder(
  img: ImageDataLike,
  tolerance: number,
  keep: number,
): ImageDataLike {
  const { width: w, height: h, data } = img;
  const tol2 = tolerance * tolerance * 3;
  const ref = [data[0], data[1], data[2]];
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

function applyTrim(img: ImageDataLike, trim: unknown): ImageDataLike {
  if (trim === undefined) return img;
  if (trim === "border") return trimBorder(img, 16, 0);
  if (typeof trim !== "object" || trim === null) bad("trim");
  const t = trim as Record<string, unknown>;
  for (const k of Object.keys(t)) {
    if (!["top", "bottom", "left", "right", "width", "height", "border"].includes(k)) bad(`trim.${k}`);
  }
  if (t.border !== undefined) {
    // Border-trim with explicit tolerance/keep/color.
    const b = t.border === true ? {} : t.border;
    if (typeof b !== "object" || b === null) bad("trim.border");
    const bo = b as Record<string, unknown>;
    for (const k of Object.keys(bo)) {
      if (!["color", "tolerance", "keep"].includes(k)) bad(`trim.border.${k}`);
    }
    const tolerance = num(bo.tolerance, "trim.border.tolerance", { min: 0, max: 255 }) ?? 16;
    const keep = num(bo.keep, "trim.border.keep", { min: 0, max: 4096 }) ?? 0;
    return trimBorder(img, tolerance, keep);
  }
  // Explicit trim: pixel amounts per side, or crop to width/height.
  const top = num(t.top, "trim.top", { min: 0, max: img.height - 1 }) ?? 0;
  const bottom = num(t.bottom, "trim.bottom", { min: 0, max: img.height - 1 }) ?? 0;
  const left = num(t.left, "trim.left", { min: 0, max: img.width - 1 }) ?? 0;
  const right = num(t.right, "trim.right", { min: 0, max: img.width - 1 }) ?? 0;
  let w = img.width - left - right;
  let h = img.height - top - bottom;
  if (t.width !== undefined) w = num(t.width, "trim.width", { min: 1, max: w })!;
  if (t.height !== undefined) h = num(t.height, "trim.height", { min: 1, max: h })!;
  return cropImage(img, left, top, w, h);
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
  const fit = op.fit === undefined ? "scale-down" : op.fit;
  if (typeof fit !== "string" || !FITS.has(fit)) bad("fit");
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
    }
  } else if (op.gravity !== undefined || fit === "crop") {
    // Gravity without dims only matters under crop semantics.
    if (op.fit === "crop") {
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
  if (op.sharpen !== undefined) img = sharpenImage(img, num(op.sharpen, "sharpen", { min: 0, max: 10 })!);

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
    const uniform = num(b.width, "border.width", { min: 0, max: 4096 });
    const l = num(b.left, "border.left", { min: 0, max: 4096 }) ?? uniform ?? 0;
    const t = num(b.top, "border.top", { min: 0, max: 4096 }) ?? uniform ?? 0;
    const r = num(b.right, "border.right", { min: 0, max: 4096 }) ?? uniform ?? 0;
    const bo = num(b.bottom, "border.bottom", { min: 0, max: 4096 }) ?? uniform ?? 0;
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
      frames[idx] = await applyTransform(frames[idx], op);
    }
  }
  return frames[0];
}
