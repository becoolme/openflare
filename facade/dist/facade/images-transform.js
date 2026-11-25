import { EngineError, adjustImage, blurImage, cropImage, drawImage, flipImage, padImage, parseColor, resizeExact, rotateImage, sharpenImage, gaussianSharpenImage } from "./images-engine.js";
export const IMAGES_TRANSFORM_MAX_PIXELS = 16_000_000;
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
    "zoom"
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
    "right"
]);
const FITS = new Set([
    "scale-down",
    "contain",
    "pad",
    "squeeze",
    "cover",
    "crop",
    "aspect-crop",
    "scale-up"
]);
const COMPOSITES = new Set([
    "over",
    "in",
    "atop",
    "out",
    "xor",
    "lighter"
]);
function bad(msg) {
    throw new EngineError(9423, `images: ${msg}`);
}
function unsupported(msg) {
    throw new EngineError(9401, `images: ${msg}`);
}
function num(v, key, { min = 0, max = Number.MAX_SAFE_INTEGER, int = false } = {}) {
    if (v === undefined) return undefined;
    if (typeof v !== "number" || !Number.isFinite(v) || v < min || v > max || int && !Number.isInteger(v)) {
        bad(`invalid ${key}`);
    }
    return v;
}
function gravityAnchor(gravity) {
    if (gravity === undefined) return {
        ax: 0.5,
        ay: 0.5,
        box: false,
        entropy: false
    };
    switch(gravity){
        case "center":
            return {
                ax: 0.5,
                ay: 0.5,
                box: false,
                entropy: false
            };
        case "left":
            return {
                ax: 0,
                ay: 0.5,
                box: false,
                entropy: false
            };
        case "right":
            return {
                ax: 1,
                ay: 0.5,
                box: false,
                entropy: false
            };
        case "top":
            return {
                ax: 0.5,
                ay: 0,
                box: false,
                entropy: false
            };
        case "bottom":
            return {
                ax: 0.5,
                ay: 1,
                box: false,
                entropy: false
            };
        case "entropy":
        case "auto":
            return {
                ax: 0.5,
                ay: 0.5,
                box: false,
                entropy: true
            };
        case "face":
            return {
                ax: 0.5,
                ay: 0.5,
                box: false,
                entropy: true
            };
        default:
            if (typeof gravity === "object" && gravity !== null) {
                const g = gravity;
                const keys = Object.keys(g);
                if (!keys.every((k)=>k === "x" || k === "y" || k === "mode")) bad("gravity object keys");
                if (g.mode !== "remainder" && g.mode !== "box-center") bad("gravity.mode");
                const x = num(g.x, "gravity.x", {
                    min: 0,
                    max: 1
                }) ?? 0.5;
                const y = num(g.y, "gravity.y", {
                    min: 0,
                    max: 1
                }) ?? 0.5;
                return {
                    ax: x,
                    ay: y,
                    box: g.mode === "box-center",
                    entropy: false
                };
            }
            bad("invalid gravity");
    }
}
function entropyCrop(img, w, h) {
    const maxX = img.width - w;
    const maxY = img.height - h;
    if (maxX <= 0 && maxY <= 0) return {
        x: 0,
        y: 0
    };
    const steps = 8;
    let best = {
        x: Math.floor(maxX / 2),
        y: Math.floor(maxY / 2)
    };
    let bestVar = -1;
    for(let gy = 0; gy <= steps; gy++){
        for(let gx = 0; gx <= steps; gx++){
            const cx = Math.floor(maxX * gx / steps);
            const cy = Math.floor(maxY * gy / steps);
            let mean = 0;
            let sq = 0;
            let n = 0;
            const stepY = Math.max(1, Math.floor(h / 64));
            const stepX = Math.max(1, Math.floor(w / 64));
            for(let py = 0; py < h; py += stepY){
                for(let px = 0; px < w; px += stepX){
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
                best = {
                    x: cx,
                    y: cy
                };
            }
        }
    }
    return best;
}
function cropRectFor(img, w, h, anchor) {
    w = Math.min(img.width, Math.max(1, Math.round(w)));
    h = Math.min(img.height, Math.max(1, Math.round(h)));
    if (anchor.entropy) {
        const { x, y } = entropyCrop(img, w, h);
        return {
            x,
            y,
            w,
            h
        };
    }
    if (anchor.box) {
        const x = Math.round(anchor.ax * img.width - w / 2);
        const y = Math.round(anchor.ay * img.height - h / 2);
        return {
            x: Math.max(0, Math.min(img.width - w, x)),
            y: Math.max(0, Math.min(img.height - h, y)),
            w,
            h
        };
    }
    return {
        x: Math.round(anchor.ax * (img.width - w)),
        y: Math.round(anchor.ay * (img.height - h)),
        w,
        h
    };
}
function trimBorder(img, tolerance, keep, refColor) {
    const { width: w, height: h, data } = img;
    const tol2 = tolerance * tolerance * 3;
    const ref = refColor ?? [
        data[0],
        data[1],
        data[2]
    ];
    const near = (i)=>{
        const dr = data[i] - ref[0];
        const dg = data[i + 1] - ref[1];
        const db = data[i + 2] - ref[2];
        return dr * dr + dg * dg + db * db <= tol2;
    };
    let top = 0;
    outerTop: while(top < h){
        for(let x = 0; x < w; x++)if (!near((top * w + x) * 4)) break outerTop;
        top++;
    }
    let bottom = h;
    outerBottom: while(bottom > top){
        for(let x = 0; x < w; x++)if (!near(((bottom - 1) * w + x) * 4)) break outerBottom;
        bottom--;
    }
    let left = 0;
    outerLeft: while(left < w){
        for(let y = top; y < bottom; y++)if (!near((y * w + left) * 4)) break outerLeft;
        left++;
    }
    let right = w;
    outerRight: while(right > left){
        for(let y = top; y < bottom; y++)if (!near((y * w + right - 1) * 4)) break outerRight;
        right--;
    }
    const x = Math.max(0, left - keep);
    const y = Math.max(0, top - keep);
    const nw = Math.min(w, right + keep) - x;
    const nh = Math.min(h, bottom + keep) - y;
    if (nw <= 0 || nh <= 0 || x === 0 && y === 0 && nw === w && nh === h) return img;
    return cropImage(img, x, y, nw, nh);
}
function trimLength(v, total, key) {
    if (typeof v === "string" && v.endsWith("p")) {
        const n = Number(v.slice(0, -1));
        if (Number.isFinite(n) && n >= 0 && n <= 100 && v.length > 1) {
            return Math.round(n / 100 * total);
        }
    }
    if (typeof v === "string" && v !== "" && !Number.isNaN(Number(v))) {
        v = Number(v);
    }
    if (typeof v !== "number" || !Number.isFinite(v) || v < 0) bad(key);
    const n = v;
    if (n > 0 && n < 1) return Math.round(n * total);
    return Math.min(Math.round(n), total);
}
function trimColorMode(img, t) {
    const ref = t.color === undefined ? undefined : (typeof t.color === "string" ? parseColor(t.color) : null) ?? bad("trim.color");
    const tolerance = num(t.tolerance, "trim.tolerance", {
        min: 0,
        max: 255,
        int: true
    }) ?? 16;
    const keep = num(t.keep, "trim.keep", {
        min: 0,
        max: 4096,
        int: true
    }) ?? 0;
    return trimBorder(img, tolerance, keep, ref === undefined ? undefined : [
        ref[0],
        ref[1],
        ref[2]
    ]);
}
function applyTrim(img, trim) {
    if (trim === undefined) return img;
    if (trim === "border" || trim === "color") return trimBorder(img, 16, 0);
    if (typeof trim !== "object" || trim === null) bad("trim");
    const t = trim;
    for (const k of Object.keys(t)){
        if (![
            "mode",
            "top",
            "bottom",
            "left",
            "right",
            "width",
            "height",
            "border",
            "color",
            "tolerance",
            "keep"
        ].includes(k)) {
            bad(`trim.${k}`);
        }
    }
    const mode = t.mode;
    if (mode === "color") {
        for (const k of Object.keys(t)){
            if (![
                "mode",
                "color",
                "tolerance",
                "keep"
            ].includes(k)) bad(`trim.${k}`);
        }
        return trimColorMode(img, t);
    }
    if (t.border !== undefined && mode === undefined) {
        const b = t.border === true ? {} : t.border;
        if (typeof b !== "object" || b === null) bad("trim.border");
        const bo = b;
        for (const k of Object.keys(bo)){
            if (![
                "color",
                "tolerance",
                "keep"
            ].includes(k)) bad(`trim.border.${k}`);
        }
        return trimColorMode(img, bo);
    }
    if (mode !== undefined && mode !== "area") bad("trim.mode");
    for (const k of Object.keys(t)){
        if (![
            "mode",
            "top",
            "bottom",
            "left",
            "right",
            "width",
            "height"
        ].includes(k)) bad(`trim.${k}`);
    }
    if (t.width !== undefined || t.height !== undefined) {
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
function borderLen(v, total, key) {
    if (v === undefined) return undefined;
    if (typeof v === "string" && v.endsWith("p")) {
        const n = Number(v.slice(0, -1));
        if (Number.isFinite(n) && n >= 0 && n <= 100 && v.length > 1) {
            return Math.round(n / 100 * total);
        }
    }
    return num(v, key, {
        min: 0,
        max: 4096
    });
}
function applySharpen(img, sharpen) {
    if (typeof sharpen === "number") {
        return sharpenImage(img, num(sharpen, "sharpen", {
            min: 0,
            max: 10
        }));
    }
    if (typeof sharpen !== "object" || sharpen === null || Array.isArray(sharpen)) bad("sharpen");
    const s = sharpen;
    for (const k of Object.keys(s)){
        if (![
            "mode",
            "amount",
            "sigma",
            "threshold"
        ].includes(k)) bad(`sharpen.${k}`);
    }
    const mode = s.mode ?? "box";
    if (mode !== "box" && mode !== "gaussian") bad("sharpen.mode");
    const amount = num(s.amount, "sharpen.amount", {
        min: 0,
        max: 10
    });
    if (amount === undefined) bad("sharpen.amount");
    if (mode === "box") {
        if (s.sigma !== undefined || s.threshold !== undefined) bad("sharpen: box takes no sigma/threshold");
        return sharpenImage(img, amount);
    }
    const sigma = num(s.sigma, "sharpen.sigma", {
        min: Number.MIN_VALUE,
        max: 5
    });
    if (sigma === undefined || sigma <= 0) bad("sharpen.sigma");
    const threshold = num(s.threshold, "sharpen.threshold", {
        min: 0,
        max: 255,
        int: true
    }) ?? 0;
    return gaussianSharpenImage(img, amount, sigma, threshold);
}
async function applyTransform(img, op) {
    for (const k of Object.keys(op)){
        if (!TRANSFORM_KEYS.has(k)) bad(`unknown transform key ${JSON.stringify(k)}`);
    }
    if (op.segment !== undefined) unsupported("segment requires an ML model the vendored engine lacks");
    if (op.zoom !== undefined && op.zoom !== 1) unsupported("zoom is not implemented");
    if (op.rotate !== undefined && ![
        0,
        90,
        180,
        270
    ].includes(op.rotate)) bad("rotate");
    img = applyTrim(img, op.trim);
    const tw = num(op.width, "width", {
        min: 1,
        max: 16384
    });
    const th = num(op.height, "height", {
        min: 1,
        max: 16384
    });
    const fitSrc = op.fit === undefined ? "scale-down" : op.fit;
    let fit;
    if (typeof fitSrc === "string") {
        fit = fitSrc;
    } else if (typeof fitSrc === "object" && fitSrc !== null && !Array.isArray(fitSrc)) {
        const f = fitSrc;
        for (const k of Object.keys(f)){
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
    const bgColor = op.background !== undefined ? (typeof op.background === "string" ? parseColor(op.background) : null) ?? bad("background color") : [
        0,
        0,
        0,
        0
    ];
    if (tw !== undefined || th !== undefined) {
        const sw = img.width;
        const sh = img.height;
        const w = tw ?? Math.max(1, Math.round(th / sh * sw));
        const h = th ?? Math.max(1, Math.round(tw / sw * sh));
        switch(fit){
            case "crop":
                {
                    const r = cropRectFor(img, w, h, anchor);
                    img = cropImage(img, r.x, r.y, r.w, r.h);
                    break;
                }
            case "cover":
                {
                    const scale = Math.max(w / sw, h / sh);
                    const cw = Math.min(sw, Math.round(w / scale));
                    const ch = Math.min(sh, Math.round(h / scale));
                    const r = cropRectFor(img, cw, ch, anchor);
                    img = await resizeExact(cropImage(img, r.x, r.y, r.w, r.h), w, h);
                    break;
                }
            case "pad":
                {
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
            case "contain":
                {
                    const scale = Math.min(w / sw, h / sh);
                    img = await resizeExact(img, Math.max(1, Math.round(sw * scale)), Math.max(1, Math.round(sh * scale)));
                    break;
                }
            case "scale-down":
                {
                    const scale = Math.min(1, Math.min(w / sw, h / sh));
                    img = await resizeExact(img, Math.max(1, Math.round(sw * scale)), Math.max(1, Math.round(sh * scale)));
                    break;
                }
            case "scale-up":
                {
                    const scale = Math.min(w / sw, h / sh);
                    if (scale > 1) {
                        img = await resizeExact(img, Math.max(1, Math.round(sw * scale)), Math.max(1, Math.round(sh * scale)));
                    }
                    break;
                }
            case "aspect-crop":
                {
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
        if (fit === "crop") {
            const r = cropRectFor(img, img.width, img.height, anchor);
            img = cropImage(img, r.x, r.y, r.w, r.h);
        }
    }
    if (op.rotate === 90 || op.rotate === 180 || op.rotate === 270) {
        img = rotateImage(img, op.rotate);
    }
    if (op.flip !== undefined) {
        if (op.flip !== "h" && op.flip !== "v" && op.flip !== "hv") bad("flip");
        img = flipImage(img, op.flip);
    }
    if (op.brightness !== undefined || op.contrast !== undefined || op.gamma !== undefined || op.saturation !== undefined) {
        img = adjustImage(img, {
            brightness: num(op.brightness, "brightness", {
                min: 0,
                max: 3
            }),
            contrast: num(op.contrast, "contrast", {
                min: 0,
                max: 3
            }),
            gamma: num(op.gamma, "gamma", {
                min: 0.01,
                max: 10
            }),
            saturation: num(op.saturation, "saturation", {
                min: 0,
                max: 3
            })
        });
    }
    if (op.blur !== undefined) img = blurImage(img, num(op.blur, "blur", {
        min: 1,
        max: 250
    }));
    if (op.sharpen !== undefined) img = applySharpen(img, op.sharpen);
    if (op.border !== undefined) {
        const b = op.border;
        if (typeof b !== "object" || b === null) bad("border");
        for (const k of Object.keys(b)){
            if (![
                "color",
                "width",
                "top",
                "bottom",
                "left",
                "right"
            ].includes(k)) bad(`border.${k}`);
        }
        const color = b.color !== undefined ? (typeof b.color === "string" ? parseColor(b.color) : null) ?? bad("border.color") : [
            0,
            0,
            0,
            255
        ];
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
function applyDraw(frames, op) {
    for (const k of Object.keys(op)){
        if (!DRAW_KEYS.has(k)) bad(`unknown draw key ${JSON.stringify(k)}`);
    }
    const drawIdx = num(op.drawImageIndex, "drawImageIndex", {
        min: 1,
        int: true
    });
    const targetIdx = num(op.targetImageIndex, "targetImageIndex", {
        min: 0,
        int: true
    });
    if (drawIdx === undefined || targetIdx === undefined) bad("draw indices");
    if (drawIdx >= frames.length || targetIdx >= frames.length) bad("draw index out of range");
    const mode = op.composite ?? "over";
    if (typeof mode !== "string" || !COMPOSITES.has(mode)) bad("composite");
    const opacity = num(op.opacity, "opacity", {
        min: 0,
        max: 1
    }) ?? 1;
    if (op.repeat !== undefined && typeof op.repeat !== "boolean" && typeof op.repeat !== "string") bad("repeat");
    frames[targetIdx] = drawImage(frames[targetIdx], frames[drawIdx], {
        top: num(op.top, "top"),
        left: num(op.left, "left"),
        bottom: num(op.bottom, "bottom"),
        right: num(op.right, "right"),
        opacity,
        repeat: op.repeat,
        composite: mode
    });
}
export async function applyTransforms(frames, ops) {
    if (frames.length === 0 || !(frames[0].data instanceof Uint8ClampedArray)) {
        bad("missing canvas image");
    }
    if (frames.length - 1 > MAX_DRAW_IMAGES) bad("too many draw images");
    for (const frame of frames){
        if (frame.width * frame.height > IMAGES_TRANSFORM_MAX_PIXELS) {
            throw new EngineError(9401, "images: source exceeds the transform pixel cap");
        }
    }
    for (const rawOp of ops){
        if (typeof rawOp !== "object" || rawOp === null || Array.isArray(rawOp)) {
            bad("transform op must be an object");
        }
        const op = rawOp;
        if ("drawImageIndex" in op || "targetImageIndex" in op) {
            applyDraw(frames, op);
        } else {
            const idx = num(op.imageIndex, "imageIndex", {
                min: 0,
                int: true
            }) ?? 0;
            if (idx >= frames.length) bad("imageIndex out of range");
            frames[idx] = await applyTransform(frames[idx], op);
        }
    }
    return frames[0];
}
