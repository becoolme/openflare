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
    "flip",
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
    const dpr = num("dpr", 0.1, 2) ?? 1;
    const w = num("width", 1, 16384);
    const h = num("height", 1, 16384);
    if (w !== undefined) op.width = Math.max(1, Math.round(w * dpr));
    if (h !== undefined) op.height = Math.max(1, Math.round(h * dpr));
    if (o.fit !== undefined) {
        const f = o.fit;
        if (typeof f === "string") {
            op.fit = f;
        } else if (typeof f === "object" && f !== null && !Array.isArray(f)) {
            const fo = f;
            for (const k of Object.keys(fo)){
                if (k !== "mode" && k !== "upscale") throw new TypeError(`cf.image: invalid fit.${k}`);
            }
            if (typeof fo.mode !== "string") throw new TypeError("cf.image: invalid fit.mode");
            op.fit = fo;
        } else {
            throw new TypeError("cf.image: invalid fit");
        }
    }
    if (o.flip !== undefined) {
        if (o.flip !== "h" && o.flip !== "v" && o.flip !== "hv") {
            throw new TypeError("cf.image: invalid flip");
        }
        op.flip = o.flip;
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
    if (o.sharpen !== undefined) {
        const s = o.sharpen;
        if (typeof s === "number") {
            op.sharpen = num("sharpen", 0, 10);
        } else if (typeof s === "object" && s !== null && !Array.isArray(s)) {
            const so = s;
            for (const k of Object.keys(so)){
                if (![
                    "mode",
                    "amount",
                    "sigma",
                    "threshold"
                ].includes(k)) {
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
            const out = {
                mode,
                amount: so.amount
            };
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
        const trimNum = (v, key)=>{
            if (typeof v === "string" && v.endsWith("p")) {
                const n = Number(v.slice(0, -1));
                if (v.length > 1 && Number.isFinite(n) && n >= 0 && n <= 100) return v;
                throw new TypeError(`cf.image: invalid trim.${key}`);
            }
            if (typeof v === "string" && v !== "" && !Number.isNaN(Number(v))) v = Number(v);
            if (typeof v !== "number" || !Number.isFinite(v) || v < 0) {
                throw new TypeError(`cf.image: invalid trim.${key}`);
            }
            if (v > 0 && v < 1) return v;
            return Math.round(v * dpr);
        };
        if (t === "border" || t === "color") {
            op.trim = "border";
        } else if (typeof t === "string") {
            const parts = t.split(",").map((s)=>Number(s));
            if (parts.length !== 4 || parts.some((n)=>!Number.isFinite(n) || n < 0)) {
                throw new TypeError("cf.image: invalid trim");
            }
            const [top, right, bottom, left] = parts.map((n)=>Math.round(n * dpr));
            op.trim = {
                top,
                right,
                bottom,
                left
            };
        } else if (typeof t === "object" && t !== null && !Array.isArray(t)) {
            const to = t;
            for (const k of Object.keys(to)){
                if (![
                    "mode",
                    "top",
                    "bottom",
                    "left",
                    "right",
                    "width",
                    "height",
                    "color",
                    "tolerance",
                    "keep"
                ].includes(k)) {
                    throw new TypeError(`cf.image: invalid trim.${k}`);
                }
            }
            const colorKeys = [
                "color",
                "tolerance",
                "keep"
            ];
            const hasColorKeys = colorKeys.some((k)=>to[k] !== undefined);
            const areaKeys = [
                "top",
                "bottom",
                "left",
                "right",
                "width",
                "height"
            ];
            const hasAreaKeys = areaKeys.some((k)=>to[k] !== undefined);
            const mode = to.mode ?? (hasColorKeys && !hasAreaKeys ? "color" : "area");
            if (mode === "color") {
                if (hasAreaKeys) throw new TypeError("cf.image: trim color mode takes no area keys");
                const out = {
                    mode: "color"
                };
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
                const out = {
                    mode: "area"
                };
                for (const k of areaKeys){
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
    for (const k of [
        "border",
        "padding"
    ]){
        if (o[k] !== undefined) {
            const v = o[k];
            const color = k === "padding" ? typeof o.background === "string" ? o.background : "rgba(0,0,0,0)" : undefined;
            if (typeof v === "number") {
                if (!Number.isFinite(v) || v < 0) throw new TypeError(`cf.image: invalid ${k}`);
                op.border = {
                    width: Math.round(v * dpr),
                    ...color !== undefined ? {
                        color
                    } : {}
                };
            } else if (typeof v === "object" && v !== null && !Array.isArray(v)) {
                const bo = v;
                const out = {};
                for (const bk of Object.keys(bo)){
                    if (![
                        "color",
                        "width",
                        "top",
                        "bottom",
                        "left",
                        "right"
                    ].includes(bk)) {
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
    if (o.compression !== undefined && o.compression !== "fast") {
        throw new TypeError("cf.image: invalid compression");
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
    if (o.compression === "fast") outputFormat = "image/jpeg";
    let outputQuality;
    if (o.quality !== undefined) {
        if (typeof o.quality === "string") {
            const TIER = {
                high: 90,
                "medium-high": 80,
                "medium-low": 60,
                low: 40
            };
            const q = TIER[o.quality];
            if (q === undefined) throw new TypeError("cf.image: invalid quality");
            outputQuality = q;
        } else {
            outputQuality = num("quality", 1, 100);
        }
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
