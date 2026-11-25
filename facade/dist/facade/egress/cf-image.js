const OPT_KEYS = new Set([
    "anim",
    "background",
    "blur",
    "border",
    "brightness",
    "compression",
    "contrast",
    "dpr",
    "draw",
    "fit",
    "format",
    "gamma",
    "gravity",
    "height",
    "image",
    "metadata",
    "onerror",
    "origin-auth",
    "padding",
    "quality",
    "rotate",
    "saturation",
    "segment",
    "sharpen",
    "slow-connection-quality",
    "trim",
    "width",
    "zoom"
]);
const SRC_MAX = 64 * 1024 * 1024;
export function cfImagePlan(request) {
    const cf = request.cf;
    if (cf === undefined || cf === null || typeof cf !== "object") return undefined;
    const image = cf.image;
    if (image === undefined || image === null) return undefined;
    if (typeof image !== "object") {
        throw new TypeError("cf.image must be an object");
    }
    const o = image;
    for (const k of Object.keys(o)){
        if (!OPT_KEYS.has(k)) throw new TypeError(`cf.image: unknown option ${k}`);
    }
    for (const k of [
        "draw",
        "image",
        "origin-auth",
        "slow-connection-quality"
    ]){
        if (o[k] !== undefined) {
            throw new TypeError(`cf.image: ${k} is not supported`);
        }
    }
    const num = (k, min, max)=>{
        const v = o[k];
        if (v === undefined) return undefined;
        if (typeof v !== "number" || !Number.isFinite(v) || v < min || v > max) {
            throw new TypeError(`cf.image: invalid ${k}`);
        }
        return v;
    };
    const op = {};
    const dpr = num("dpr", 0.1, 4) ?? 1;
    const w = num("width", 1, 16384);
    const h = num("height", 1, 16384);
    if (w !== undefined) op.width = Math.max(1, Math.round(w * dpr));
    if (h !== undefined) op.height = Math.max(1, Math.round(h * dpr));
    if (o.fit !== undefined) {
        if (typeof o.fit !== "string") throw new TypeError("cf.image: invalid fit");
        op.fit = o.fit;
    }
    if (o.gravity !== undefined) {
        const g = o.gravity;
        if (typeof g === "string") {
            op.gravity = g;
        } else if (typeof g === "object" && g !== null && typeof g.x === "number" && typeof g.y === "number") {
            const gr = g;
            op.gravity = {
                x: gr.x,
                y: gr.y,
                mode: gr.mode ?? "remainder"
            };
        } else {
            throw new TypeError("cf.image: invalid gravity");
        }
    }
    if (o.rotate !== undefined) {
        const r = num("rotate", 0, 360);
        if (![
            0,
            90,
            180,
            270
        ].includes(r)) throw new TypeError("cf.image: invalid rotate");
        op.rotate = r;
    }
    if (o.blur !== undefined) op.blur = num("blur", 1, 250);
    if (o.sharpen !== undefined) op.sharpen = num("sharpen", 0, 10);
    if (o.brightness !== undefined) op.brightness = num("brightness", 0, 3);
    if (o.contrast !== undefined) op.contrast = num("contrast", 0, 3);
    if (o.gamma !== undefined) op.gamma = num("gamma", 0.01, 10);
    if (o.saturation !== undefined) op.saturation = num("saturation", 0, 3);
    if (o.zoom !== undefined) op.zoom = num("zoom", 0.1, 8);
    if (o.background !== undefined) {
        if (typeof o.background !== "string" || o.background.length > 64) {
            throw new TypeError("cf.image: invalid background");
        }
        op.background = o.background;
    }
    if (o.segment !== undefined) op.segment = o.segment;
    if (o.trim !== undefined) {
        const t = o.trim;
        if (t === "border") {
            op.trim = "border";
        } else if (typeof t === "string") {
            const parts = t.split(",").map((s)=>Number(s));
            if (parts.length !== 4 || parts.some((n)=>!Number.isFinite(n) || n < 0)) {
                throw new TypeError("cf.image: invalid trim");
            }
            const [top, right, bottom, left] = parts;
            op.trim = {
                top,
                right,
                bottom,
                left
            };
        } else {
            throw new TypeError("cf.image: invalid trim");
        }
    }
    for (const k of [
        "border",
        "padding"
    ]){
        if (o[k] !== undefined) {
            const v = o[k];
            if (typeof v === "number") {
                op.border = {
                    width: v
                };
            } else if (typeof v === "object" && v !== null) {
                op.border = v;
            } else {
                throw new TypeError(`cf.image: invalid ${k}`);
            }
        }
    }
    let outputFormat;
    let json = false;
    if (o.format !== undefined) {
        switch(o.format){
            case "avif":
                outputFormat = "image/avif";
                break;
            case "webp":
                outputFormat = "image/webp";
                break;
            case "jpeg":
            case "baseline-jpeg":
                outputFormat = "image/jpeg";
                break;
            case "png":
            case "png-force":
                outputFormat = "image/png";
                break;
            case "json":
                json = true;
                break;
            case "gif":
            case "rgb":
            case "rgba":
                outputFormat = `image/${o.format}`;
                break;
            default:
                throw new TypeError("cf.image: invalid format");
        }
    }
    let outputQuality;
    if (o.quality !== undefined) {
        outputQuality = num("quality", 1, 100);
    }
    const anim = o.anim === true;
    const metadata = o.metadata === undefined ? null : o.metadata === "none" || o.metadata === "keep" || o.metadata === "copyright" ? o.metadata : (()=>{
        throw new TypeError("cf.image: invalid metadata");
    })();
    return {
        transforms: JSON.stringify([
            op
        ]),
        outputFormat,
        outputQuality,
        anim,
        metadata,
        json,
        onerrorRedirect: o.onerror === "redirect"
    };
}
export function cfImageBody(src, plan) {
    const fd = new FormData();
    fd.append("image", new Blob([
        src
    ]), "source");
    fd.append("transforms", plan.transforms);
    fd.append("output_format", plan.outputFormat ?? "image/jpeg");
    if (plan.outputQuality !== undefined) fd.append("output_quality", String(plan.outputQuality));
    if (plan.anim) fd.append("anim", "true");
    if (plan.metadata !== null) fd.append("metadata", plan.metadata);
    return fd;
}
export { SRC_MAX };
