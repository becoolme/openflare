import initResizeWasm, { resize as wasmResize } from "../third_party/jsquash/resize/squoosh_resize.js";
import resizeWasmBytes from "../third_party/jsquash/resize/squoosh_resize_bg.wasm";
import initPngWasm, { decode as pngDecodeWasm, encode as pngEncodeWasm } from "../third_party/jsquash/png/squoosh_png.js";
import pngWasmBytes from "../third_party/jsquash/png/squoosh_png_bg.wasm";
import mozjpegDecFactory from "../third_party/jsquash/jpeg/mozjpeg_dec.js";
import mozjpegDecWasmBytes from "../third_party/jsquash/jpeg/mozjpeg_dec.wasm";
import mozjpegEncFactory from "../third_party/jsquash/jpeg/mozjpeg_enc.js";
import mozjpegEncWasmBytes from "../third_party/jsquash/jpeg/mozjpeg_enc.wasm";
import webpDecFactory from "../third_party/jsquash/webp/webp_dec.js";
import webpDecWasmBytes from "../third_party/jsquash/webp/webp_dec.wasm";
import webpEncFactory from "../third_party/jsquash/webp/webp_enc_simd.js";
import webpEncWasmBytes from "../third_party/jsquash/webp/webp_enc_simd.wasm";
import avifDecFactory from "../third_party/jsquash/avif/avif_dec.js";
import avifDecWasmBytes from "../third_party/jsquash/avif/avif_dec.wasm";
import avifEncFactory from "../third_party/jsquash/avif/avif_enc.js";
import avifEncWasmBytes from "../third_party/jsquash/avif/avif_enc.wasm";
import { decodeBmp, decodeGif } from "./images-decode.js";
import { EngineError } from "./images-wire.js";
export { EngineError };
if (typeof globalThis.ImageData === "undefined") {
    globalThis.ImageData = class {
        data;
        width;
        height;
        constructor(data, width, height){
            this.data = data;
            this.width = width;
            this.height = height;
        }
    };
}
function instantiateEmscripten(factory, wasmModule) {
    const mod = factory({
        noInitialRun: true,
        instantiateWasm: (imports, callback)=>{
            const instance = new WebAssembly.Instance(wasmModule, imports);
            callback(instance);
            return instance.exports;
        }
    });
    return Promise.resolve(mod.ready).then(()=>mod);
}
let engineInit = null;
let jpegDec;
let jpegEnc;
let webpDec;
let webpEnc;
let avifDec;
let avifEnc;
const RESIZE_METHODS = [
    "triangle",
    "catrom",
    "mitchell",
    "lanczos3"
];
async function ensureEngine() {
    if (!engineInit) {
        engineInit = (async ()=>{
            await Promise.all([
                initResizeWasm(resizeWasmBytes),
                initPngWasm(pngWasmBytes)
            ]);
            [jpegDec, jpegEnc, webpDec, webpEnc, avifDec, avifEnc] = await Promise.all([
                instantiateEmscripten(mozjpegDecFactory, mozjpegDecWasmBytes),
                instantiateEmscripten(mozjpegEncFactory, mozjpegEncWasmBytes),
                instantiateEmscripten(webpDecFactory, webpDecWasmBytes),
                instantiateEmscripten(webpEncFactory, webpEncWasmBytes),
                instantiateEmscripten(avifDecFactory, avifDecWasmBytes),
                instantiateEmscripten(avifEncFactory, avifEncWasmBytes)
            ]);
        })();
    }
    return engineInit;
}
function frame(data, width, height) {
    return {
        data: data instanceof Uint8ClampedArray ? data : new Uint8ClampedArray(data.buffer, data.byteOffset, data.length),
        width,
        height
    };
}
export async function decodeImage(bytes, format) {
    await ensureEngine();
    let out;
    switch(format){
        case "jpeg":
            out = jpegDec.decode(bytes, false);
            break;
        case "png":
            out = pngDecodeWasm(bytes);
            break;
        case "webp":
            out = webpDec.decode(bytes);
            break;
        case "avif":
            out = avifDec.decode(bytes, 8);
            break;
        case "gif":
            out = decodeGif(bytes);
            break;
        case "bmp":
            out = decodeBmp(bytes);
            break;
        default:
            throw new EngineError(9401, `images: ${format} decode is not supported by the vendored engine`);
    }
    if (!out || !(out.data instanceof Uint8ClampedArray)) {
        throw new EngineError(9420, `images: ${format} decode failed (corrupt or unsupported variant)`);
    }
    return out;
}
export const ENCODE_FORMATS = [
    "jpeg",
    "png",
    "webp",
    "avif",
    "rgb",
    "rgba"
];
const JPEG_OPTIONS = {
    baseline: false,
    arithmetic: false,
    progressive: true,
    optimize_coding: true,
    smoothing: 0,
    color_space: 3,
    quant_table: 3,
    trellis_multipass: false,
    trellis_opt_zero: false,
    trellis_opt_table: false,
    trellis_loops: 1,
    auto_subsample: true,
    chroma_subsample: 2,
    separate_chroma_quality: false,
    chroma_quality: 75
};
const WEBP_OPTIONS = {
    target_size: 0,
    target_PSNR: 0,
    method: 4,
    sns_strength: 50,
    filter_strength: 60,
    filter_sharpness: 0,
    filter_type: 1,
    partitions: 0,
    segments: 4,
    pass: 1,
    show_compressed: 0,
    preprocessing: 0,
    autofilter: 0,
    partition_limit: 0,
    alpha_compression: 1,
    alpha_filtering: 1,
    alpha_quality: 100,
    lossless: 0,
    exact: 0,
    image_hint: 0,
    emulate_jpeg_size: 0,
    thread_level: 0,
    low_memory: 0,
    near_lossless: 100,
    use_delta_palette: 0,
    use_sharp_yuv: 0
};
export async function encodeImage(img, format, quality = 85) {
    await ensureEngine();
    switch(format){
        case "jpeg":
            {
                const out = jpegEnc.encode(img.data, img.width, img.height, {
                    ...JPEG_OPTIONS,
                    quality: Math.max(1, Math.min(100, quality)),
                    chroma_quality: Math.max(1, Math.min(100, quality))
                });
                if (!out) throw new EngineError(9523, "images: jpeg encode failed");
                return {
                    bytes: out,
                    contentType: "image/jpeg"
                };
            }
        case "png":
            {
                const out = pngEncodeWasm(new Uint8Array(img.data.buffer.slice(0)), img.width, img.height, 8);
                if (!out) throw new EngineError(9523, "images: png encode failed");
                return {
                    bytes: new Uint8Array(out),
                    contentType: "image/png"
                };
            }
        case "webp":
            {
                const out = webpEnc.encode(img.data, img.width, img.height, {
                    ...WEBP_OPTIONS,
                    quality: Math.max(1, Math.min(100, quality))
                });
                if (!out) throw new EngineError(9523, "images: webp encode failed");
                return {
                    bytes: out,
                    contentType: "image/webp"
                };
            }
        case "rgb":
            {
                const out = new Uint8Array(img.width * img.height * 3);
                for(let i = 0, j = 0; i < out.length; i += 3, j += 4){
                    out[i] = img.data[j];
                    out[i + 1] = img.data[j + 1];
                    out[i + 2] = img.data[j + 2];
                }
                return {
                    bytes: out,
                    contentType: "application/octet-stream"
                };
            }
        case "rgba":
            return {
                bytes: new Uint8Array(img.data.buffer.slice(0)),
                contentType: "application/octet-stream"
            };
        case "avif":
            {
                const out = avifEnc.encode(new Uint8Array(img.data.buffer, img.data.byteOffset, img.data.length), img.width, img.height, {
                    quality: Math.max(0, Math.min(100, quality)),
                    qualityAlpha: -1,
                    denoiseLevel: 0,
                    tileColsLog2: 0,
                    tileRowsLog2: 0,
                    speed: 6,
                    subsample: 1,
                    chromaDeltaQ: false,
                    sharpness: 0,
                    tune: 0,
                    enableSharpYUV: false,
                    bitDepth: 8,
                    lossless: false
                });
                if (!out) throw new EngineError(9523, "images: avif encode failed");
                return {
                    bytes: out,
                    contentType: "image/avif"
                };
            }
        default:
            throw new EngineError(9401, `images: ${format} output is not supported by the vendored engine`);
    }
}
export function cropImage(img, x, y, w, h) {
    x = Math.max(0, Math.min(img.width, Math.floor(x)));
    y = Math.max(0, Math.min(img.height, Math.floor(y)));
    w = Math.max(1, Math.min(img.width - x, Math.floor(w)));
    h = Math.max(1, Math.min(img.height - y, Math.floor(h)));
    const out = new Uint8ClampedArray(w * h * 4);
    for(let row = 0; row < h; row++){
        out.set(img.data.subarray(((y + row) * img.width + x) * 4, ((y + row) * img.width + x + w) * 4), row * w * 4);
    }
    return frame(out, w, h);
}
export function padImage(img, left, top, right, bottom, color) {
    const w = img.width + left + right;
    const h = img.height + top + bottom;
    const out = new Uint8ClampedArray(w * h * 4);
    for(let i = 0; i < out.length; i += 4){
        out[i] = color[0];
        out[i + 1] = color[1];
        out[i + 2] = color[2];
        out[i + 3] = color[3];
    }
    for(let row = 0; row < img.height; row++){
        out.set(img.data.subarray(row * img.width * 4, (row + 1) * img.width * 4), ((top + row) * w + left) * 4);
    }
    return frame(out, w, h);
}
export async function resizeExact(img, w, h) {
    await ensureEngine();
    if (w === img.width && h === img.height) return img;
    const outW = Math.max(1, Math.round(w));
    const outH = Math.max(1, Math.round(h));
    const out = wasmResize(new Uint8Array(img.data.buffer, img.data.byteOffset, img.data.length), img.width, img.height, outW, outH, RESIZE_METHODS.indexOf("lanczos3"), true, true);
    if (!out || !(out instanceof Uint8ClampedArray)) {
        throw new EngineError(9523, "images: resize failed");
    }
    return frame(out, outW, outH);
}
export function flipImage(img, mode) {
    const { width: w, height: h } = img;
    const out = new Uint8ClampedArray(img.data.length);
    const hFlip = mode === "h" || mode === "hv";
    const vFlip = mode === "v" || mode === "hv";
    for(let y = 0; y < h; y++){
        for(let x = 0; x < w; x++){
            const sx = hFlip ? w - 1 - x : x;
            const sy = vFlip ? h - 1 - y : y;
            out.set(img.data.subarray((sy * w + sx) * 4, (sy * w + sx) * 4 + 4), (y * w + x) * 4);
        }
    }
    return frame(out, w, h);
}
export function rotateImage(img, deg) {
    const { width: w, height: h } = img;
    const [ow, oh] = deg === 180 ? [
        w,
        h
    ] : [
        h,
        w
    ];
    const out = new Uint8ClampedArray(img.data.length);
    for(let y = 0; y < h; y++){
        for(let x = 0; x < w; x++){
            let dx, dy;
            if (deg === 90) {
                dx = h - 1 - y;
                dy = x;
            } else if (deg === 180) {
                dx = w - 1 - x;
                dy = h - 1 - y;
            } else {
                dx = y;
                dy = w - 1 - x;
            }
            out.set(img.data.subarray((y * w + x) * 4, (y * w + x) * 4 + 4), (dy * ow + dx) * 4);
        }
    }
    return frame(out, ow, oh);
}
export function blurImage(img, sigma) {
    const radius = Math.max(1, Math.min(125, Math.round(sigma / 2)));
    const { width: w, height: h } = img;
    let src = img.data;
    for(let pass = 0; pass < 3; pass++){
        const tmp = new Uint8ClampedArray(src.length);
        for(let y = 0; y < h; y++){
            const sum = [
                0,
                0,
                0,
                0
            ];
            let count = 0;
            for(let x = -radius; x <= radius; x++){
                if (x >= 0 && x < w) {
                    const i = (y * w + x) * 4;
                    sum[0] += src[i];
                    sum[1] += src[i + 1];
                    sum[2] += src[i + 2];
                    sum[3] += src[i + 3];
                    count++;
                }
            }
            for(let x = 0; x < w; x++){
                const o = (y * w + x) * 4;
                tmp[o] = sum[0] / count;
                tmp[o + 1] = sum[1] / count;
                tmp[o + 2] = sum[2] / count;
                tmp[o + 3] = sum[3] / count;
                const add = x + radius + 1;
                const sub = x - radius;
                if (add < w) {
                    const i = (y * w + add) * 4;
                    sum[0] += src[i];
                    sum[1] += src[i + 1];
                    sum[2] += src[i + 2];
                    sum[3] += src[i + 3];
                    count++;
                }
                if (sub >= 0) {
                    const i = (y * w + sub) * 4;
                    sum[0] -= src[i];
                    sum[1] -= src[i + 1];
                    sum[2] -= src[i + 2];
                    sum[3] -= src[i + 3];
                    count--;
                }
            }
        }
        const dst = new Uint8ClampedArray(src.length);
        for(let x = 0; x < w; x++){
            const sum = [
                0,
                0,
                0,
                0
            ];
            let count = 0;
            for(let y = -radius; y <= radius; y++){
                if (y >= 0 && y < h) {
                    const i = (y * w + x) * 4;
                    sum[0] += tmp[i];
                    sum[1] += tmp[i + 1];
                    sum[2] += tmp[i + 2];
                    sum[3] += tmp[i + 3];
                    count++;
                }
            }
            for(let y = 0; y < h; y++){
                const o = (y * w + x) * 4;
                dst[o] = sum[0] / count;
                dst[o + 1] = sum[1] / count;
                dst[o + 2] = sum[2] / count;
                dst[o + 3] = sum[3] / count;
                const add = y + radius + 1;
                const sub = y - radius;
                if (add < h) {
                    const i = (add * w + x) * 4;
                    sum[0] += tmp[i];
                    sum[1] += tmp[i + 1];
                    sum[2] += tmp[i + 2];
                    sum[3] += tmp[i + 3];
                    count++;
                }
                if (sub >= 0) {
                    const i = (sub * w + x) * 4;
                    sum[0] -= tmp[i];
                    sum[1] -= tmp[i + 1];
                    sum[2] -= tmp[i + 2];
                    sum[3] -= tmp[i + 3];
                    count--;
                }
            }
        }
        src = dst;
    }
    return frame(src, w, h);
}
function boxBlur3(img) {
    const { width: w, height: h, data: src } = img;
    const out = new Uint8ClampedArray(src.length);
    for(let y = 0; y < h; y++){
        for(let x = 0; x < w; x++){
            let r = 0, g = 0, b = 0, n = 0;
            for(let dy = -1; dy <= 1; dy++){
                const yy = y + dy;
                if (yy < 0 || yy >= h) continue;
                for(let dx = -1; dx <= 1; dx++){
                    const xx = x + dx;
                    if (xx < 0 || xx >= w) continue;
                    const i = (yy * w + xx) * 4;
                    r += src[i];
                    g += src[i + 1];
                    b += src[i + 2];
                    n++;
                }
            }
            const o = (y * w + x) * 4;
            out[o] = r / n;
            out[o + 1] = g / n;
            out[o + 2] = b / n;
            out[o + 3] = src[o + 3];
        }
    }
    return frame(out, w, h);
}
export function gaussianBlur(img, sigma) {
    const { width: w, height: h, data: src } = img;
    const radius = Math.max(1, Math.ceil(3 * sigma));
    const kernel = new Float64Array(2 * radius + 1);
    let ksum = 0;
    for(let i = -radius; i <= radius; i++){
        const v = Math.exp(-(i * i) / (2 * sigma * sigma));
        kernel[i + radius] = v;
        ksum += v;
    }
    for(let i = 0; i < kernel.length; i++)kernel[i] /= ksum;
    const tmp = new Float64Array(src.length);
    for(let y = 0; y < h; y++){
        for(let x = 0; x < w; x++){
            let r = 0, g = 0, b = 0, wsum = 0;
            for(let k = -radius; k <= radius; k++){
                const xx = x + k;
                if (xx < 0 || xx >= w) continue;
                const kv = kernel[k + radius];
                const i = (y * w + xx) * 4;
                r += src[i] * kv;
                g += src[i + 1] * kv;
                b += src[i + 2] * kv;
                wsum += kv;
            }
            const o = (y * w + x) * 4;
            tmp[o] = r / wsum;
            tmp[o + 1] = g / wsum;
            tmp[o + 2] = b / wsum;
            tmp[o + 3] = src[o + 3];
        }
    }
    const out = new Uint8ClampedArray(src.length);
    for(let y = 0; y < h; y++){
        for(let x = 0; x < w; x++){
            let r = 0, g = 0, b = 0, wsum = 0;
            for(let k = -radius; k <= radius; k++){
                const yy = y + k;
                if (yy < 0 || yy >= h) continue;
                const kv = kernel[k + radius];
                const i = (yy * w + x) * 4;
                r += tmp[i] * kv;
                g += tmp[i + 1] * kv;
                b += tmp[i + 2] * kv;
                wsum += kv;
            }
            const o = (y * w + x) * 4;
            out[o] = r / wsum;
            out[o + 1] = g / wsum;
            out[o + 2] = b / wsum;
            out[o + 3] = src[o + 3];
        }
    }
    return frame(out, w, h);
}
function unsharpMask(img, blurred, amount, threshold) {
    const out = new Uint8ClampedArray(img.data.length);
    for(let i = 0; i < out.length; i += 4){
        for(let c = 0; c < 3; c++){
            const d = img.data[i + c] - blurred.data[i + c];
            out[i + c] = Math.abs(d) >= threshold ? img.data[i + c] + amount * d : img.data[i + c];
        }
        out[i + 3] = img.data[i + 3];
    }
    return frame(out, img.width, img.height);
}
export function sharpenImage(img, amount) {
    return unsharpMask(img, boxBlur3(img), Math.max(0, Math.min(10, amount)), 0);
}
export function gaussianSharpenImage(img, amount, sigma, threshold) {
    return unsharpMask(img, gaussianBlur(img, sigma), Math.max(0, Math.min(10, amount)), Math.max(0, Math.min(255, Math.round(threshold))));
}
export function adjustImage(img, { brightness, contrast, gamma, saturation }) {
    const lut = new Uint8ClampedArray(256);
    const g = gamma !== undefined && gamma > 0 ? 1 / gamma : 1;
    const c = contrast !== undefined ? Math.tan((contrast - 1) * Math.PI / 4 + Math.PI / 4) * 0.5 + 0.5 : 1;
    const b = brightness ?? 1;
    for(let i = 0; i < 256; i++){
        let v = Math.pow(i / 255, g) * 255;
        v = (v - 128) * c + 128;
        v *= b;
        lut[i] = v;
    }
    const out = new Uint8ClampedArray(img.data.length);
    for(let i = 0; i < out.length; i += 4){
        let r = lut[img.data[i]];
        let gch = lut[img.data[i + 1]];
        let bl = lut[img.data[i + 2]];
        if (saturation !== undefined) {
            const luma = 0.2126 * r + 0.7152 * gch + 0.0722 * bl;
            r = luma + (r - luma) * saturation;
            gch = luma + (gch - luma) * saturation;
            bl = luma + (bl - luma) * saturation;
        }
        out[i] = r;
        out[i + 1] = gch;
        out[i + 2] = bl;
        out[i + 3] = img.data[i + 3];
    }
    return frame(out, img.width, img.height);
}
const COMPOSITE_MODES = [
    "over",
    "in",
    "atop",
    "out",
    "xor",
    "lighter"
];
export function composite(base, over, dx, dy, opacity = 1, mode = "over") {
    const sx = Math.max(0, -dx);
    const sy = Math.max(0, -dy);
    const ex = Math.min(over.width, base.width - dx);
    const ey = Math.min(over.height, base.height - dy);
    for(let y = sy; y < ey; y++){
        for(let x = sx; x < ex; x++){
            const si = (y * over.width + x) * 4;
            const di = ((y + dy) * base.width + (x + dx)) * 4;
            const sa = over.data[si + 3] / 255 * opacity;
            const da = base.data[di + 3] / 255;
            let fa, fb;
            switch(mode){
                case "over":
                    fa = 1;
                    fb = 1 - sa;
                    break;
                case "in":
                    fa = da;
                    fb = 0;
                    break;
                case "atop":
                    fa = da;
                    fb = 1 - sa;
                    break;
                case "out":
                    fa = 1 - da;
                    fb = 0;
                    break;
                case "xor":
                    fa = 1 - da;
                    fb = 1 - sa;
                    break;
                case "lighter":
                    fa = 1;
                    fb = 1;
                    break;
            }
            const outA = fa * sa + fb * da;
            if (mode === "lighter") {
                for(let c = 0; c < 3; c++){
                    base.data[di + c] = Math.min(255, over.data[si + c] * sa + base.data[di + c] * da);
                }
                base.data[di + 3] = Math.min(255, (sa + da) * 255);
                continue;
            }
            for(let c = 0; c < 3; c++){
                base.data[di + c] = outA <= 1e-9 ? 0 : (over.data[si + c] * sa * fa + base.data[di + c] * da * fb) / outA;
            }
            base.data[di + 3] = Math.min(255, outA * 255);
        }
    }
}
export function drawImage(base, over, { top, left, bottom, right, opacity = 1, repeat, composite: mode = "over" }) {
    const dx = left !== undefined ? Math.round(left) : right !== undefined ? base.width - over.width - Math.round(right) : 0;
    const dy = top !== undefined ? Math.round(top) : bottom !== undefined ? base.height - over.height - Math.round(bottom) : 0;
    const repX = repeat === true || repeat === "x" || repeat === "xy";
    const repY = repeat === true || repeat === "y" || repeat === "xy";
    if (!repX && !repY) {
        composite(base, over, dx, dy, opacity, mode);
        return base;
    }
    const startX = repX ? dx % over.width - over.width : dx;
    const startY = repY ? dy % over.height - over.height : dy;
    for(let y = startY; y < base.height; y += repY ? over.height : base.height){
        for(let x = startX; x < base.width; x += repX ? over.width : base.width){
            composite(base, over, x, y, opacity, mode);
        }
    }
    return base;
}
export function parseColor(value) {
    const v = value.trim().toLowerCase();
    const named = {
        black: [
            0,
            0,
            0,
            255
        ],
        white: [
            255,
            255,
            255,
            255
        ],
        red: [
            255,
            0,
            0,
            255
        ],
        green: [
            0,
            128,
            0,
            255
        ],
        blue: [
            0,
            0,
            255,
            255
        ],
        transparent: [
            0,
            0,
            0,
            0
        ]
    };
    if (v in named) return named[v];
    const hex = /^#([0-9a-f]{3,8})$/.exec(v);
    if (hex) {
        const h = hex[1];
        if (h.length === 3 || h.length === 4) {
            return [
                parseInt(h[0] + h[0], 16),
                parseInt(h[1] + h[1], 16),
                parseInt(h[2] + h[2], 16),
                h.length === 4 ? parseInt(h[3] + h[3], 16) : 255
            ];
        }
        if (h.length === 6 || h.length === 8) {
            return [
                parseInt(h.slice(0, 2), 16),
                parseInt(h.slice(2, 4), 16),
                parseInt(h.slice(4, 6), 16),
                h.length === 8 ? parseInt(h.slice(6, 8), 16) : 255
            ];
        }
    }
    const rgb = /^rgba?\(([^)]+)\)$/.exec(v);
    if (rgb) {
        const parts = rgb[1].split(",").map((s)=>parseFloat(s.trim()));
        if (parts.length >= 3 && parts.every((n)=>Number.isFinite(n))) {
            return [
                parts[0],
                parts[1],
                parts[2],
                parts.length > 3 ? Math.round(parts[3] * 255) : 255
            ];
        }
    }
    return null;
}
