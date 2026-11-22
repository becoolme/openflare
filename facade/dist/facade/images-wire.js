export function imagesError(code, message, status = 400) {
    return new Response(message, {
        status,
        headers: {
            "cf-images-binding": `err=${code}`
        }
    });
}
export const IMAGES_BODY_MAX_BYTES = 64 * 1024 * 1024;
export const IMAGES_MAX_PIXELS = 70_000_000;
export class EngineError extends Error {
    code;
    constructor(code, message){
        super(message);
        this.code = code;
    }
}
export function sniffImage(buf) {
    const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    if (buf.length >= 24 && dv.getUint32(0) === 0x89504e47 && dv.getUint32(4) === 0x0d0a1a0a) {
        return {
            format: "png",
            width: dv.getUint32(16),
            height: dv.getUint32(20)
        };
    }
    if (buf.length >= 10 && dv.getUint32(0) === 0x47494638 && (buf[4] === 0x37 || buf[4] === 0x39)) {
        return {
            format: "gif",
            width: dv.getUint16(6, true),
            height: dv.getUint16(8, true)
        };
    }
    if (buf.length >= 26 && buf[0] === 0x42 && buf[1] === 0x4d) {
        return {
            format: "bmp",
            width: Math.abs(dv.getInt32(18, true)),
            height: Math.abs(dv.getInt32(22, true))
        };
    }
    if (buf.length >= 12 && dv.getUint32(4) === 0x66747970 && (dv.getUint32(8) === 0x61766966 || dv.getUint32(8) === 0x61766973)) {
        const scanEnd = Math.min(buf.length - 16, 65536);
        for(let off = 12; off < scanEnd; off++){
            if (dv.getUint32(off + 4) === 0x69737065) {
                const w = dv.getUint32(off + 12);
                const h = dv.getUint32(off + 16);
                if (w > 0 && h > 0) return {
                    format: "avif",
                    width: w,
                    height: h
                };
            }
        }
        return {
            format: "avif",
            width: 0,
            height: 0
        };
    }
    if (buf.length >= 30 && dv.getUint32(0) === 0x52494646 && dv.getUint32(8) === 0x57454250) {
        const fourcc = dv.getUint32(12);
        if (fourcc === 0x56503858) {
            const w = (buf[24] | buf[25] << 8 | buf[26] << 16) + 1;
            const h = (buf[27] | buf[28] << 8 | buf[29] << 16) + 1;
            return {
                format: "webp",
                width: w,
                height: h
            };
        }
        if (fourcc === 0x56503820 && buf.length >= 30) {
            return {
                format: "webp",
                width: dv.getUint16(26, true) & 0x3fff,
                height: dv.getUint16(28, true) & 0x3fff
            };
        }
        if (fourcc === 0x5650384c && buf.length >= 25) {
            const b = (buf[21] | buf[22] << 8 | buf[23] << 16 | buf[24] << 24) >>> 0;
            return {
                format: "webp",
                width: (b & 0x3fff) + 1,
                height: (b >> 14 & 0x3fff) + 1
            };
        }
        return {
            format: "webp",
            width: 0,
            height: 0
        };
    }
    if (buf.length >= 4 && dv.getUint16(0) === 0xffd8) {
        let off = 2;
        while(off + 9 < buf.length){
            if (buf[off] !== 0xff) break;
            const marker = buf[off + 1];
            if (marker >= 0xc0 && marker <= 0xcf && marker !== 0xc4 && marker !== 0xc8 && marker !== 0xcc) {
                return {
                    format: "jpeg",
                    width: dv.getUint16(off + 7),
                    height: dv.getUint16(off + 5)
                };
            }
            const len = dv.getUint16(off + 2);
            off += 2 + len;
        }
        return {
            format: "jpeg",
            width: 0,
            height: 0
        };
    }
    return null;
}
export function sniffSvg(buf) {
    const head = new TextDecoder().decode(buf.subarray(0, Math.min(buf.length, 4096)));
    const trimmed = head.trimStart();
    return trimmed.startsWith("<svg") || trimmed.startsWith("<?xml");
}
