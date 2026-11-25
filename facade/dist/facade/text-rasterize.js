import { initWasm, Resvg } from "../third_party/resvg/index.js";
import resvgWasm from "../third_party/resvg/index_bg.wasm";
import { EngineError } from "./images-wire.js";
import { FONT_MAX_BYTES, TEXT_MAX_DIM, looksLikeFont } from "./text-spec.js";
let resvgReady;
function ensureResvg() {
    return resvgReady ??= initWasm(resvgWasm);
}
const escapeXml = (s)=>s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
export async function rasterizeText(spec, fontBytes) {
    if (fontBytes.length === 0 || fontBytes.length > FONT_MAX_BYTES) {
        throw new EngineError(9420, "images: font empty or over the 20MiB cap");
    }
    if (!looksLikeFont(fontBytes)) {
        throw new EngineError(9420, "images: font not a recognized font file");
    }
    await ensureResvg();
    const cw = Math.min(TEXT_MAX_DIM, Math.ceil(spec.size * (spec.text.length + 2)));
    const ch = Math.min(TEXT_MAX_DIM, Math.ceil(spec.size * 3));
    const pad = Math.ceil(spec.size);
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${cw}" height="${ch}">` + `<text x="${pad}" y="${pad + spec.size}" font-size="${spec.size}" fill="${spec.color}">` + escapeXml(spec.text) + "</text></svg>";
    let resvg;
    try {
        resvg = new Resvg(svg, {
            font: {
                fontBuffers: [
                    fontBytes
                ],
                loadSystemFonts: false
            }
        });
    } catch (e) {
        throw new EngineError(9420, `images: font could not be parsed: ${e}`);
    }
    const bbox = resvg.innerBBox();
    if (bbox === undefined || bbox.width < 1 || bbox.height < 1) {
        return {
            data: new Uint8ClampedArray(4),
            width: 1,
            height: 1
        };
    }
    if (bbox.width >= TEXT_MAX_DIM || bbox.height >= TEXT_MAX_DIM) {
        throw new EngineError(9423, "images: rendered text exceeds the 4096px bound");
    }
    let img;
    try {
        resvg.cropByBBox(bbox);
        img = resvg.render();
    } catch (e) {
        throw new EngineError(9420, `images: text rasterization failed: ${e}`);
    }
    const src = img.pixels;
    const data = new Uint8ClampedArray(src.length);
    for(let i = 0; i < src.length; i += 4){
        const a = src[i + 3];
        if (a === 0) continue;
        if (a === 255) {
            data[i] = src[i];
            data[i + 1] = src[i + 1];
            data[i + 2] = src[i + 2];
        } else {
            data[i] = Math.min(255, Math.round(src[i] * 255 / a));
            data[i + 1] = Math.min(255, Math.round(src[i + 1] * 255 / a));
            data[i + 2] = Math.min(255, Math.round(src[i + 2] * 255 / a));
        }
        data[i + 3] = a;
    }
    return {
        data,
        width: img.width,
        height: img.height
    };
}
