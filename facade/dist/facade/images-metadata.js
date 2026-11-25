const enc = new TextEncoder();
const dec = new TextDecoder();
export const METADATA_MODES = new Set([
    "none",
    "copyright",
    "keep"
]);
function ascii(bytes, off, len) {
    return String.fromCharCode(...bytes.subarray(off, off + len));
}
function startsWith(bytes, off, prefix) {
    return prefix.every((b, i)=>bytes[off + i] === b);
}
const EXIF_PREFIX = new Uint8Array([
    0x45,
    0x78,
    0x69,
    0x66,
    0,
    0
]);
const XMP_PREFIX = enc.encode("http://ns.adobe.com/xap/1.0/\0");
const PS_PREFIX = enc.encode("Photoshop 3.0\0");
const XMP_KEYWORD = "XML:com.adobe.xmp";
function extractJpeg(buf, meta) {
    const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let off = 2;
    while(off + 4 <= buf.length && buf[off] === 0xff){
        const marker = buf[off + 1];
        if (marker === 0xda || marker === 0xd9) break;
        if (marker === 0x01 || marker >= 0xd0 && marker <= 0xd7) {
            off += 2;
            continue;
        }
        const segLen = dv.getUint16(off + 2);
        const end = off + 2 + segLen;
        if (end > buf.length) break;
        const payload = buf.subarray(off + 4, end);
        if (marker === 0xe1) {
            if (!meta.exif && startsWith(buf, off + 4, EXIF_PREFIX)) {
                meta.exif = payload.slice(EXIF_PREFIX.length);
            } else if (!meta.xmp && startsWith(buf, off + 4, XMP_PREFIX)) {
                meta.xmp = payload.slice(XMP_PREFIX.length);
            }
        } else if (marker === 0xed && !meta.iptc && startsWith(buf, off + 4, PS_PREFIX)) {
            meta.iptc = payload.slice(PS_PREFIX.length);
        }
        off = end;
    }
}
function extractPng(buf, meta) {
    const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let off = 8;
    while(off + 12 <= buf.length){
        const len = dv.getUint32(off);
        const type = ascii(buf, off + 4, 4);
        const data = off + 8;
        if (data + len + 4 > buf.length) break;
        if (type === "eXIf" && !meta.exif) {
            meta.exif = buf.slice(data, data + len);
        } else if (type === "iTXt" && !meta.xmp) {
            let p = data;
            const keyEnd = buf.indexOf(0, p);
            if (keyEnd > 0 && ascii(buf, p, keyEnd - p) === XMP_KEYWORD) {
                p = keyEnd + 1;
                const compressed = buf[p] === 1;
                p += 2;
                const langEnd = buf.indexOf(0, p);
                p = langEnd + 1;
                const transEnd = buf.indexOf(0, p);
                p = transEnd + 1;
                if (!compressed && p <= data + len) meta.xmp = buf.slice(p, data + len);
            }
        } else if (type === "IEND") {
            break;
        }
        off = data + len + 4;
    }
}
function extractWebp(buf, meta) {
    const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let off = 12;
    while(off + 8 <= buf.length){
        const fourcc = ascii(buf, off, 4);
        const len = dv.getUint32(off + 4, true);
        const data = off + 8;
        if (data + len > buf.length) break;
        if (fourcc === "EXIF" && !meta.exif) meta.exif = buf.slice(data, data + len);
        if (fourcc === "XMP " && !meta.xmp) meta.xmp = buf.slice(data, data + len);
        off = data + len + (len & 1);
    }
}
export function extractMetadata(buf, format) {
    const meta = {};
    switch(format){
        case "jpeg":
            extractJpeg(buf, meta);
            break;
        case "png":
            extractPng(buf, meta);
            break;
        case "webp":
            extractWebp(buf, meta);
            break;
        case "avif":
            extractAvif(buf, meta);
            break;
    }
    return meta;
}
function exifCopyrightOnly(tiff) {
    if (tiff.length < 8) return undefined;
    const dv = new DataView(tiff.buffer, tiff.byteOffset, tiff.byteLength);
    const le = tiff[0] === 0x49 && tiff[1] === 0x49;
    if (!(le || tiff[0] === 0x4d && tiff[1] === 0x4d)) return undefined;
    if (dv.getUint16(2, le) !== 42) return undefined;
    const ifd = dv.getUint32(4, le);
    if (ifd + 2 > tiff.length) return undefined;
    const count = dv.getUint16(ifd, le);
    for(let i = 0; i < count; i++){
        const e = ifd + 2 + i * 12;
        if (e + 12 > tiff.length) break;
        if (dv.getUint16(e, le) !== 0x8298) continue;
        const type = dv.getUint16(e + 2, le);
        const n = dv.getUint32(e + 4, le);
        const value = n <= 4 ? tiff.slice(e + 8, e + 8 + n) : (()=>{
            const at = dv.getUint32(e + 8, le);
            return at + n <= tiff.length ? tiff.slice(at, at + n) : undefined;
        })();
        if (!value || type !== 2) return undefined;
        const out = new Uint8Array(8 + 2 + 12 + 4 + (n > 4 ? n : 0));
        const odv = new DataView(out.buffer);
        out.set([
            0x49,
            0x49
        ]);
        odv.setUint16(2, 42, true);
        odv.setUint32(4, 8, true);
        odv.setUint16(8, 1, true);
        odv.setUint16(10, 0x8298, true);
        odv.setUint16(12, 2, true);
        odv.setUint32(14, n, true);
        if (n <= 4) out.set(value, 18);
        else {
            odv.setUint32(18, 26, true);
            out.set(value, 26);
        }
        return out;
    }
    return undefined;
}
const IPTC_XMP_MAP = [
    [
        0x05,
        "dc:title",
        "alt"
    ],
    [
        0x19,
        "dc:subject",
        "bag"
    ],
    [
        0x37,
        "photoshop:DateCreated",
        null
    ],
    [
        0x50,
        "dc:creator",
        "seq"
    ],
    [
        0x55,
        "photoshop:AuthorsPosition",
        null
    ],
    [
        0x5a,
        "photoshop:City",
        null
    ],
    [
        0x5f,
        "photoshop:State",
        null
    ],
    [
        0x65,
        "photoshop:Country",
        null
    ],
    [
        0x67,
        "photoshop:TransmissionReference",
        null
    ],
    [
        0x69,
        "photoshop:Headline",
        null
    ],
    [
        0x6e,
        "photoshop:Credit",
        null
    ],
    [
        0x73,
        "photoshop:Source",
        null
    ],
    [
        0x74,
        "dc:rights",
        "alt"
    ],
    [
        0x78,
        "dc:description",
        "alt"
    ],
    [
        0x7a,
        "photoshop:CaptionWriter",
        null
    ]
];
const XML_ENTITIES = {
    "<": "&lt;",
    ">": "&gt;",
    "&": "&amp;",
    "'": "&apos;",
    '"': "&quot;"
};
function xmlEscape(s) {
    let out = "";
    for (const c of s)out += XML_ENTITIES[c] ?? c;
    return out;
}
function iptcDatasets(irb) {
    const out = new Map();
    const dv = new DataView(irb.buffer, irb.byteOffset, irb.byteLength);
    let off = 0;
    while(off + 6 <= irb.length){
        if (ascii(irb, off, 4) !== "8BIM") break;
        const id = dv.getUint16(off + 4);
        let p = off + 6;
        const nameLen = irb[p];
        p += 1 + nameLen + (nameLen + 1 & 1);
        const size = dv.getUint32(p);
        const data = p + 4;
        if (data + size > irb.length) break;
        if (id === 0x0404) {
            const idv = new DataView(irb.buffer, irb.byteOffset + data, size);
            let q = 0;
            while(q + 5 <= size && irb[data + q] === 0x1c){
                const record = irb[data + q + 1];
                const ds = irb[data + q + 2];
                const n = idv.getUint16(q + 3);
                q += 5;
                if (record === 2 && q + n <= size) {
                    const list = out.get(ds) ?? [];
                    list.push(dec.decode(irb.subarray(data + q, data + q + n)));
                    out.set(ds, list);
                }
                q += n;
            }
        }
        off = data + size + (size & 1);
    }
    return out;
}
export function iptcToXmp(irb) {
    const ds = iptcDatasets(irb);
    if (ds.size === 0) return undefined;
    const props = [];
    for (const [id, prop, list] of IPTC_XMP_MAP){
        const values = ds.get(id);
        if (!values?.length) continue;
        if (list === null) {
            props.push(`<${prop}>${xmlEscape(values[0])}</${prop}>`);
        } else {
            const tag = list === "bag" ? "rdf:Bag" : list === "seq" ? "rdf:Seq" : "rdf:Alt";
            const item = list === "alt" ? 'rdf:li xml:lang="x-default"' : "rdf:li";
            props.push(`<${prop}><${tag}>` + values.map((v)=>`<${item}>${xmlEscape(v)}</rdf:li>`).join("") + `</${tag}></${prop}>`);
        }
    }
    if (props.length === 0) return undefined;
    const xml = '<?xpacket begin="\ufeff" id="W5M0MpCehiHzreSzNTczkc9d"?>' + '<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF ' + 'xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" ' + 'xmlns:dc="http://purl.org/dc/elements/1.1/" ' + 'xmlns:photoshop="http://ns.adobe.com/photoshop/1.0/">' + `<rdf:Description rdf:about="">${props.join("")}</rdf:Description>` + "</rdf:RDF></x:xmpmeta><?xpacket end=\"w\"?>";
    return enc.encode(xml);
}
const PNG_CRC_TABLE = (()=>{
    const t = new Uint32Array(256);
    for(let n = 0; n < 256; n++){
        let c = n;
        for(let k = 0; k < 8; k++)c = c & 1 ? 0xedb88320 ^ c >>> 1 : c >>> 1;
        t[n] = c;
    }
    return t;
})();
function pngCrc(bytes) {
    let crc = 0xffffffff;
    for (const b of bytes)crc = PNG_CRC_TABLE[(crc ^ b) & 0xff] ^ crc >>> 8;
    return (crc ^ 0xffffffff) >>> 0;
}
function pngChunk(type, data) {
    const out = new Uint8Array(12 + data.length);
    const dv = new DataView(out.buffer);
    dv.setUint32(0, data.length);
    out.set(enc.encode(type), 4);
    out.set(data, 8);
    dv.setUint32(8 + data.length, pngCrc(out.subarray(4, 8 + data.length)));
    return out;
}
function jpegSegment(marker, payload) {
    const out = new Uint8Array(4 + payload.length);
    const dv = new DataView(out.buffer);
    out[0] = 0xff;
    out[1] = marker;
    dv.setUint16(2, payload.length + 2);
    out.set(payload, 4);
    return out;
}
function insertJpegMeta(out, meta) {
    const segs = [];
    if (meta.exif) {
        const p = new Uint8Array(EXIF_PREFIX.length + meta.exif.length);
        p.set(EXIF_PREFIX);
        p.set(meta.exif, EXIF_PREFIX.length);
        segs.push(jpegSegment(0xe1, p));
    }
    if (meta.xmp) {
        const p = new Uint8Array(XMP_PREFIX.length + meta.xmp.length);
        p.set(XMP_PREFIX);
        p.set(meta.xmp, XMP_PREFIX.length);
        segs.push(jpegSegment(0xe1, p));
    }
    if (meta.iptc) {
        const p = new Uint8Array(PS_PREFIX.length + meta.iptc.length);
        p.set(PS_PREFIX);
        p.set(meta.iptc, PS_PREFIX.length);
        segs.push(jpegSegment(0xed, p));
    }
    if (segs.length === 0) return out;
    const dv = new DataView(out.buffer, out.byteOffset, out.byteLength);
    let at = 2;
    if (out.length > 6 && out[2] === 0xff && out[3] === 0xe0) {
        at = 4 + dv.getUint16(4);
    }
    const total = segs.reduce((s, x)=>s + x.length, 0);
    const res = new Uint8Array(out.length + total);
    res.set(out.subarray(0, at));
    let p = at;
    for (const s of segs){
        res.set(s, p);
        p += s.length;
    }
    res.set(out.subarray(at), p);
    return res;
}
function insertPngMeta(out, meta) {
    const chunks = [];
    if (meta.exif) chunks.push(pngChunk("eXIf", meta.exif));
    if (meta.xmp) {
        const kw = enc.encode(XMP_KEYWORD);
        const data = new Uint8Array(kw.length + 1 + 2 + 1 + 1 + meta.xmp.length);
        data.set(kw);
        let p = kw.length + 1;
        p += 2;
        p += 1;
        p += 1;
        data.set(meta.xmp, p);
        chunks.push(pngChunk("iTXt", data));
    }
    if (chunks.length === 0) return out;
    const dv = new DataView(out.buffer, out.byteOffset, out.byteLength);
    const ihdrEnd = 8 + 8 + dv.getUint32(8) + 4;
    const total = chunks.reduce((s, c)=>s + c.length, 0);
    const res = new Uint8Array(out.length + total);
    res.set(out.subarray(0, ihdrEnd));
    let p = ihdrEnd;
    for (const c of chunks){
        res.set(c, p);
        p += c.length;
    }
    res.set(out.subarray(ihdrEnd), p);
    return res;
}
function riffChunk(fourcc, data) {
    const out = new Uint8Array(8 + data.length + (data.length & 1));
    const dv = new DataView(out.buffer);
    out.set(enc.encode(fourcc));
    dv.setUint32(4, data.length, true);
    out.set(data, 8);
    return out;
}
function insertWebpMeta(out, meta, width, height) {
    const extra = [];
    if (meta.exif) extra.push(riffChunk("EXIF", meta.exif));
    if (meta.xmp) extra.push(riffChunk("XMP ", meta.xmp));
    if (extra.length === 0) return out;
    const dv = new DataView(out.buffer, out.byteOffset, out.byteLength);
    let flags = 0;
    let start = 12;
    if (out.length >= 30 && ascii(out, 12, 4) === "VP8X") {
        flags = out[20];
        start = 30;
    }
    const chunks = [];
    let off = start;
    while(off + 8 <= out.length){
        const len = dv.getUint32(off + 4, true);
        const end = off + 8 + len + (len & 1);
        if (end > out.length) return out;
        const fourcc = ascii(out, off, 4);
        if (fourcc === "ALPH" || fourcc === "VP8L") flags |= 0x10;
        chunks.push(out.slice(off, end));
        off = end;
    }
    if (meta.exif) flags |= 0x08;
    if (meta.xmp) flags |= 0x04;
    const vp8x = new Uint8Array(18);
    const xdv = new DataView(vp8x.buffer);
    vp8x.set(enc.encode("VP8X"));
    xdv.setUint32(4, 10, true);
    vp8x[8] = flags;
    const w = width - 1, h = height - 1;
    vp8x[12] = w & 0xff;
    vp8x[13] = w >> 8 & 0xff;
    vp8x[14] = w >> 16 & 0xff;
    vp8x[15] = h & 0xff;
    vp8x[16] = h >> 8 & 0xff;
    vp8x[17] = h >> 16 & 0xff;
    const body = [
        vp8x,
        ...chunks,
        ...extra
    ];
    const bodyLen = body.reduce((s, c)=>s + c.length, 0);
    const res = new Uint8Array(12 + bodyLen);
    res.set(enc.encode("RIFF"));
    new DataView(res.buffer).setUint32(4, 4 + bodyLen, true);
    res.set(enc.encode("WEBP"), 8);
    let p = 12;
    for (const c of body){
        res.set(c, p);
        p += c.length;
    }
    return res;
}
export function attachMetadata(out, format, meta, mode, width, height) {
    if (mode === "none") return out;
    let m = meta;
    if (mode === "copyright") {
        const exif = meta.exif && exifCopyrightOnly(meta.exif);
        m = exif ? {
            exif
        } : {};
    } else {
        if (format !== "jpeg" && !m.xmp && m.iptc) {
            const mapped = iptcToXmp(m.iptc);
            if (mapped) m = {
                ...m,
                xmp: mapped
            };
        }
        if (format !== "jpeg") m = {
            ...m,
            iptc: undefined
        };
    }
    switch(format){
        case "jpeg":
            return insertJpegMeta(out, m);
        case "png":
            return insertPngMeta(out, m);
        case "webp":
            return insertWebpMeta(out, m, width, height);
        case "avif":
            return m.exif ? insertAvifExif(out, m.exif) : out;
        default:
            return out;
    }
}
function bmBoxes(buf, start, end) {
    const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    const out = [];
    let off = start;
    while(off + 8 <= end){
        let size = dv.getUint32(off);
        const type = ascii(buf, off + 4, 4);
        let header = 8;
        if (size === 1) {
            size = Number(dv.getBigUint64(off + 8));
            header = 16;
        } else if (size === 0) size = end - off;
        if (size < header || off + size > end) return out;
        out.push({
            type,
            start: off,
            header,
            end: off + size
        });
        off += size;
    }
    return out;
}
function bmBox(type, payload) {
    const out = new Uint8Array(8 + payload.length);
    new DataView(out.buffer).setUint32(0, out.length);
    out.set(enc.encode(type), 4);
    out.set(payload, 8);
    return out;
}
function bmInt(buf, off, size) {
    let v = 0;
    for(let i = 0; i < size; i++)v = v * 256 + buf[off + i];
    return v;
}
function bmPutInt(buf, off, size, v) {
    for(let i = size - 1; i >= 0; i--){
        buf[off + i] = v & 0xff;
        v >>= 8;
    }
}
function parseIloc(p) {
    if (p.length < 8) return undefined;
    const dv = new DataView(p.buffer, p.byteOffset, p.byteLength);
    const version = p[0];
    if (version > 2) return undefined;
    const offSize = p[4] >> 4, lenSize = p[4] & 0xf;
    const baseSize = p[5] >> 4, indexSize = version >= 1 ? p[5] & 0xf : 0;
    let off = 6;
    const count = version < 2 ? dv.getUint16(off) : dv.getUint32(off);
    off += version < 2 ? 2 : 4;
    const entries = [];
    for(let i = 0; i < count; i++){
        if (off + 4 > p.length) return undefined;
        const id = version < 2 ? dv.getUint16(off) : dv.getUint32(off);
        off += version < 2 ? 2 : 4;
        let construction = 0;
        if (version >= 1) {
            construction = dv.getUint16(off) & 0xf;
            off += 2;
        }
        off += 2;
        const base = bmInt(p, off, baseSize);
        off += baseSize;
        const n = dv.getUint16(off);
        off += 2;
        const extents = [];
        for(let e = 0; e < n; e++){
            let index = 0;
            if (version >= 1 && indexSize > 0) {
                index = bmInt(p, off, indexSize);
                off += indexSize;
            }
            const offset = bmInt(p, off, offSize);
            off += offSize;
            const length = bmInt(p, off, lenSize);
            off += lenSize;
            if (off > p.length) return undefined;
            extents.push({
                offset,
                length,
                index
            });
        }
        entries.push({
            id,
            construction,
            base,
            extents
        });
    }
    return {
        version,
        offSize,
        lenSize,
        baseSize,
        indexSize,
        entries
    };
}
function serializeIloc(l) {
    let n = 6 + (l.version < 2 ? 2 : 4);
    for (const e of l.entries){
        n += (l.version < 2 ? 2 : 4) + (l.version >= 1 ? 2 : 0) + 2 + l.baseSize + 2;
        n += e.extents.length * ((l.version >= 1 ? l.indexSize : 0) + l.offSize + l.lenSize);
    }
    const p = new Uint8Array(n);
    const dv = new DataView(p.buffer);
    p[0] = l.version;
    p[4] = l.offSize << 4 | l.lenSize;
    p[5] = l.baseSize << 4 | (l.version >= 1 ? l.indexSize : 0);
    let off = 6;
    if (l.version < 2) {
        dv.setUint16(off, l.entries.length);
        off += 2;
    } else {
        dv.setUint32(off, l.entries.length);
        off += 4;
    }
    for (const e of l.entries){
        if (l.version < 2) {
            dv.setUint16(off, e.id);
            off += 2;
        } else {
            dv.setUint32(off, e.id);
            off += 4;
        }
        if (l.version >= 1) {
            dv.setUint16(off, e.construction);
            off += 2;
        }
        dv.setUint16(off, 0);
        off += 2;
        bmPutInt(p, off, l.baseSize, e.base);
        off += l.baseSize;
        dv.setUint16(off, e.extents.length);
        off += 2;
        for (const x of e.extents){
            if (l.version >= 1 && l.indexSize > 0) {
                bmPutInt(p, off, l.indexSize, x.index);
                off += l.indexSize;
            }
            bmPutInt(p, off, l.offSize, x.offset);
            off += l.offSize;
            bmPutInt(p, off, l.lenSize, x.length);
            off += l.lenSize;
        }
    }
    return p;
}
function parseInfe(p) {
    if (p.length < 8) return undefined;
    const dv = new DataView(p.buffer, p.byteOffset, p.byteLength);
    const version = p[0];
    if (version !== 2 && version !== 3) return undefined;
    const id = version === 2 ? dv.getUint16(4) : dv.getUint32(4);
    const itemType = ascii(p, 8, 4);
    let off = 12;
    while(off < p.length && p[off] !== 0)off++;
    off++;
    let contentType = "";
    if (itemType === "mime") {
        const start = off;
        while(off < p.length && p[off] !== 0)off++;
        contentType = dec.decode(p.subarray(start, off));
    }
    return {
        id,
        itemType,
        contentType
    };
}
function extractAvif(buf, meta) {
    const top = bmBoxes(buf, 0, buf.length);
    const mb = top.find((b)=>b.type === "meta");
    if (!mb) return;
    const kids = bmBoxes(buf, mb.start + mb.header + 4, mb.end);
    const iinf = kids.find((c)=>c.type === "iinf");
    const ilocB = kids.find((c)=>c.type === "iloc");
    const idat = kids.find((c)=>c.type === "idat");
    if (!iinf || !ilocB) return;
    const iloc = parseIloc(buf.subarray(ilocB.start + ilocB.header, ilocB.end));
    if (!iloc) return;
    const items = new Map();
    const iinfVer = buf[iinf.start + iinf.header];
    const infes = bmBoxes(buf, iinf.start + iinf.header + (iinfVer === 0 ? 6 : 8), iinf.end);
    for (const e of infes){
        if (e.type !== "infe") continue;
        const parsed = parseInfe(buf.subarray(e.start + e.header, e.end));
        if (parsed) items.set(parsed.id, parsed);
    }
    const readItem = (id)=>{
        const entry = iloc.entries.find((x)=>x.id === id);
        if (!entry || entry.construction === 2) return undefined;
        const parts = [];
        for (const x of entry.extents){
            const at = entry.construction === 1 && idat ? idat.start + idat.header + entry.base + x.offset : entry.base + x.offset;
            if (at + x.length > buf.length) return undefined;
            parts.push(buf.subarray(at, at + x.length));
        }
        const total = parts.reduce((s, x)=>s + x.length, 0);
        const out = new Uint8Array(total);
        let p = 0;
        for (const x of parts){
            out.set(x, p);
            p += x.length;
        }
        return out;
    };
    for (const [id, info] of items){
        if (info.itemType === "Exif" && !meta.exif) {
            const raw = readItem(id);
            if (raw && raw.length > 8) {
                const skip = 4 + bmInt(raw, 0, 4);
                if (skip < raw.length) meta.exif = raw.slice(skip);
            }
        } else if (info.itemType === "mime" && info.contentType === "application/rdf+xml" && !meta.xmp) {
            meta.xmp = readItem(id);
        }
    }
}
function insertAvifExif(buf, tiff) {
    const top = bmBoxes(buf, 0, buf.length);
    const mb = top.find((b)=>b.type === "meta");
    if (!mb) return buf;
    const contentStart = mb.start + mb.header + 4;
    const kids = bmBoxes(buf, contentStart, mb.end);
    const iinfB = kids.find((c)=>c.type === "iinf");
    const ilocB = kids.find((c)=>c.type === "iloc");
    const irefB = kids.find((c)=>c.type === "iref");
    const pitmB = kids.find((c)=>c.type === "pitm");
    if (!iinfB || !ilocB || !pitmB) return buf;
    const iloc = parseIloc(buf.subarray(ilocB.start + ilocB.header, ilocB.end));
    if (!iloc || iloc.offSize < 4) return buf;
    const pitmP = buf.subarray(pitmB.start + pitmB.header, pitmB.end);
    const primaryId = pitmP[0] === 0 ? new DataView(pitmP.buffer, pitmP.byteOffset).getUint16(4) : new DataView(pitmP.buffer, pitmP.byteOffset).getUint32(4);
    const iinfP = buf.subarray(iinfB.start + iinfB.header, iinfB.end);
    const iinfVer = iinfP[0];
    const infes = bmBoxes(buf, iinfB.start + iinfB.header + (iinfVer === 0 ? 6 : 8), iinfB.end);
    let maxId = 0;
    for (const e of infes){
        const parsed = parseInfe(buf.subarray(e.start + e.header, e.end));
        if (!parsed) return buf;
        if (parsed.itemType === "Exif") return buf;
        if (parsed.id > maxId) maxId = parsed.id;
    }
    const newId = maxId + 1;
    if (newId > 0xffff) return buf;
    const payload = new Uint8Array(4 + tiff.length);
    payload.set(tiff, 4);
    const infePayload = new Uint8Array(4 + 2 + 2 + 4 + 5);
    const idv = new DataView(infePayload.buffer);
    infePayload[0] = 2;
    idv.setUint16(4, newId);
    infePayload.set(enc.encode("Exif"), 8);
    infePayload.set(enc.encode("Exif\0"), 12);
    const infe = bmBox("infe", infePayload);
    const iinfHeaderLen = iinfVer === 0 ? 6 : 8;
    const oldIinf = buf.subarray(iinfB.start + iinfB.header, iinfB.end);
    const iinf2 = new Uint8Array(oldIinf.length + infe.length);
    iinf2.set(oldIinf);
    if (iinfVer === 0) new DataView(iinf2.buffer).setUint16(4, infes.length + 1);
    else new DataView(iinf2.buffer).setUint32(4, infes.length + 1);
    iinf2.set(infe, oldIinf.length);
    const iinf2Box = bmBox("iinf", iinf2);
    const exifEntry = {
        id: newId,
        construction: 0,
        base: 0,
        extents: [
            {
                offset: 0,
                length: payload.length,
                index: 0
            }
        ]
    };
    const idWidth = (irefB ? buf[irefB.start + irefB.header] : 0) === 0 ? 2 : 4;
    const cdsc = new Uint8Array(idWidth * 3);
    const cdv = new DataView(cdsc.buffer);
    bmPutInt(cdsc, 0, idWidth, newId);
    if (idWidth === 2) {
        cdv.setUint16(2, 1);
        bmPutInt(cdsc, 4, 2, primaryId);
    } else {
        cdv.setUint32(4, 1);
        bmPutInt(cdsc, 8, 4, primaryId);
    }
    const cdscBox = bmBox("cdsc", cdsc);
    let iref2Box;
    if (irefB) {
        const old = buf.subarray(irefB.start + irefB.header, irefB.end);
        const merged = new Uint8Array(old.length + cdscBox.length);
        merged.set(old);
        merged.set(cdscBox, old.length);
        iref2Box = bmBox("iref", merged);
    } else {
        const full = new Uint8Array(4 + cdscBox.length);
        full.set(cdscBox, 4);
        iref2Box = bmBox("iref", full);
    }
    const iloc2Pre = {
        ...iloc,
        entries: [
            ...iloc.entries,
            exifEntry
        ]
    };
    let iloc2 = serializeIloc(iloc2Pre);
    const metaGrowth = iinf2Box.length - (iinfB.end - iinfB.start) + (bmBox("iloc", iloc2).length - (ilocB.end - ilocB.start)) + (iref2Box.length - (irefB ? irefB.end - irefB.start : 0));
    const metaEnd = mb.end;
    for (const e of iloc2Pre.entries){
        if (e === exifEntry) continue;
        if (e.construction !== 0) continue;
        if (e.base >= metaEnd) e.base += metaGrowth;
        for (const x of e.extents)if (x.offset >= metaEnd) x.offset += metaGrowth;
    }
    exifEntry.extents[0].offset = buf.length + metaGrowth;
    iloc2 = serializeIloc(iloc2Pre);
    const parts = [];
    let sawIref = false;
    for (const c of kids){
        if (c.type === "iinf") parts.push(iinf2Box);
        else if (c.type === "iloc") parts.push(bmBox("iloc", iloc2));
        else if (c.type === "iref") {
            parts.push(iref2Box);
            sawIref = true;
        } else parts.push(buf.subarray(c.start, c.end));
    }
    if (!sawIref) parts.push(iref2Box);
    const bodyLen = parts.reduce((s, x)=>s + x.length, 0);
    const metaOut = new Uint8Array(8 + 4 + bodyLen);
    new DataView(metaOut.buffer).setUint32(0, metaOut.length);
    metaOut.set(enc.encode("meta"), 4);
    metaOut.set(buf.subarray(mb.start + mb.header, mb.start + mb.header + 4), 8);
    let p = 12;
    for (const x of parts){
        metaOut.set(x, p);
        p += x.length;
    }
    const res = new Uint8Array(buf.length + metaGrowth + payload.length);
    res.set(buf.subarray(0, mb.start));
    res.set(metaOut, mb.start);
    res.set(buf.subarray(mb.end), mb.start + metaOut.length);
    res.set(payload, buf.length + metaGrowth);
    return res;
}
