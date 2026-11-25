const SVG_NS = "http://www.w3.org/2000/svg";
const XLINK_NS = "http://www.w3.org/1999/xlink";
export class HushError extends Error {
}
const ALLOWED_SVG_ELEMENTS = new Set([
    "a",
    "altGlyph",
    "altGlyphDef",
    "altGlyphItem",
    "animate",
    "animateColor",
    "animateMotion",
    "animateTransform",
    "circle",
    "clipPath",
    "color-profile",
    "defs",
    "desc",
    "ellipse",
    "feBlend",
    "feColorMatrix",
    "feComponentTransfer",
    "feComposite",
    "feConvolveMatrix",
    "feDiffuseLighting",
    "feDisplacementMap",
    "feDistantLight",
    "feDropShadow",
    "feFlood",
    "feFuncA",
    "feFuncB",
    "feFuncG",
    "feFuncR",
    "feGaussianBlur",
    "feImage",
    "feMerge",
    "feMergeNode",
    "feMorphology",
    "feOffset",
    "fePointLight",
    "feSpecularLighting",
    "feSpotLight",
    "feTile",
    "feTurbulence",
    "filter",
    "flowDiv",
    "flowLine",
    "flowPara",
    "flowRegion",
    "flowRoot",
    "flowSpan",
    "flowTref",
    "font",
    "g",
    "glyph",
    "glyphRef",
    "hkern",
    "image",
    "line",
    "linearGradient",
    "marker",
    "mask",
    "meshgradient",
    "meshpatch",
    "meshrow",
    "mpath",
    "mPath",
    "path",
    "pattern",
    "polygon",
    "polyline",
    "radialGradient",
    "rect",
    "set",
    "solidColor",
    "stop",
    "svg",
    "switch",
    "symbol",
    "text",
    "textPath",
    "title",
    "tref",
    "tspan",
    "use",
    "view",
    "vkern"
]);
const MAY_USE_HREF = new Set([
    "radialGradient",
    "linearGradient",
    "image",
    "use",
    "pattern",
    "feImage"
]);
const ATTRIBUTE_TYPES = new Map([
    [
        "attributeName",
        "attrname"
    ],
    [
        "fr",
        "any"
    ],
    [
        "vector-effect",
        "keyword"
    ],
    [
        "solid-color",
        "any"
    ],
    [
        "mask-type",
        "keyword"
    ],
    [
        "filterPrimitiveUnits",
        "keyword"
    ],
    [
        "attributeType",
        "keyword"
    ],
    [
        "blend",
        "any"
    ],
    [
        "animateColor",
        "any"
    ],
    [
        "animateMotion",
        "any"
    ],
    [
        "animateTransform",
        "any"
    ],
    [
        "arabic-form",
        "any"
    ],
    [
        "bbox",
        "any"
    ],
    [
        "begin",
        "any"
    ],
    [
        "by",
        "any"
    ],
    [
        "color-profile",
        "url"
    ],
    [
        "unicode",
        "any"
    ],
    [
        "description",
        "text"
    ],
    [
        "dur",
        "any"
    ],
    [
        "end",
        "any"
    ],
    [
        "format",
        "any"
    ],
    [
        "from",
        "any"
    ],
    [
        "g1",
        "any"
    ],
    [
        "g2",
        "any"
    ],
    [
        "glyph-name",
        "any"
    ],
    [
        "glyphRef",
        "any"
    ],
    [
        "hidden",
        "keyword"
    ],
    [
        "id",
        "keyword"
    ],
    [
        "in",
        "any"
    ],
    [
        "in2",
        "any"
    ],
    [
        "kernelMatrix",
        "any"
    ],
    [
        "keyPoints",
        "any"
    ],
    [
        "keySplines",
        "any"
    ],
    [
        "keyTimes",
        "any"
    ],
    [
        "line-height",
        "any"
    ],
    [
        "local",
        "any"
    ],
    [
        "max",
        "any"
    ],
    [
        "min",
        "any"
    ],
    [
        "name",
        "any"
    ],
    [
        "orient",
        "any"
    ],
    [
        "orientation",
        "any"
    ],
    [
        "origin",
        "any"
    ],
    [
        "panose-1",
        "any"
    ],
    [
        "path",
        "any"
    ],
    [
        "repeatCount",
        "any"
    ],
    [
        "repeatDur",
        "any"
    ],
    [
        "result",
        "any"
    ],
    [
        "side",
        "keyword"
    ],
    [
        "string",
        "any"
    ],
    [
        "tabindex",
        "number"
    ],
    [
        "tableValues",
        "any"
    ],
    [
        "to",
        "urlfunc"
    ],
    [
        "transform-origin",
        "any"
    ],
    [
        "u1",
        "any"
    ],
    [
        "u2",
        "any"
    ],
    [
        "unicode-range",
        "any"
    ],
    [
        "values",
        "any"
    ],
    [
        "widths",
        "any"
    ],
    [
        "accent-height",
        "number"
    ],
    [
        "accumulate",
        "keyword"
    ],
    [
        "additive",
        "keyword"
    ],
    [
        "alignment-baseline",
        "keyword"
    ],
    [
        "alphabetic",
        "number"
    ],
    [
        "amplitude",
        "number"
    ],
    [
        "arcrole",
        "url"
    ],
    [
        "ascent",
        "number"
    ],
    [
        "azimuth",
        "number"
    ],
    [
        "base",
        "url"
    ],
    [
        "baseFrequency",
        "any"
    ],
    [
        "baseline-shift",
        "any"
    ],
    [
        "baseProfile",
        "text"
    ],
    [
        "bias",
        "number"
    ],
    [
        "calcMode",
        "keyword"
    ],
    [
        "cap-height",
        "number"
    ],
    [
        "class",
        "any"
    ],
    [
        "clip",
        "urlfunc"
    ],
    [
        "clip-path",
        "urlfunc"
    ],
    [
        "clip-rule",
        "any"
    ],
    [
        "clipPathUnits",
        "keyword"
    ],
    [
        "color",
        "any"
    ],
    [
        "color-interpolation",
        "keyword"
    ],
    [
        "color-interpolation-filters",
        "keyword"
    ],
    [
        "color-rendering",
        "keyword"
    ],
    [
        "cursor",
        "urlfunc"
    ],
    [
        "cx",
        "any"
    ],
    [
        "cy",
        "any"
    ],
    [
        "d",
        "any"
    ],
    [
        "descent",
        "number"
    ],
    [
        "diffuseConstant",
        "number"
    ],
    [
        "direction",
        "keyword"
    ],
    [
        "display",
        "keyword"
    ],
    [
        "divisor",
        "number"
    ],
    [
        "dominant-baseline",
        "keyword"
    ],
    [
        "dx",
        "any"
    ],
    [
        "dy",
        "any"
    ],
    [
        "edgeMode",
        "keyword"
    ],
    [
        "elevation",
        "number"
    ],
    [
        "enable-background",
        "any"
    ],
    [
        "exponent",
        "number"
    ],
    [
        "externalResourcesRequired",
        "keyword"
    ],
    [
        "fill",
        "urlfunc"
    ],
    [
        "fill-opacity",
        "any"
    ],
    [
        "fill-rule",
        "any"
    ],
    [
        "filter",
        "urlfunc"
    ],
    [
        "filterRes",
        "any"
    ],
    [
        "filterUnits",
        "keyword"
    ],
    [
        "flood-color",
        "any"
    ],
    [
        "flood-opacity",
        "any"
    ],
    [
        "font-family",
        "any"
    ],
    [
        "font-size",
        "any"
    ],
    [
        "font-size-adjust",
        "any"
    ],
    [
        "font-stretch",
        "keyword"
    ],
    [
        "font-style",
        "keyword"
    ],
    [
        "font-variant",
        "keyword"
    ],
    [
        "font-weight",
        "keyword"
    ],
    [
        "fx",
        "any"
    ],
    [
        "fy",
        "any"
    ],
    [
        "glyph-orientation-horizontal",
        "any"
    ],
    [
        "glyph-orientation-vertical",
        "any"
    ],
    [
        "gradientTransform",
        "any"
    ],
    [
        "gradientUnits",
        "keyword"
    ],
    [
        "hanging",
        "number"
    ],
    [
        "height",
        "any"
    ],
    [
        "horiz-adv-x",
        "number"
    ],
    [
        "horiz-origin-x",
        "number"
    ],
    [
        "horiz-origin-y",
        "number"
    ],
    [
        "href",
        "url"
    ],
    [
        "ideographic",
        "number"
    ],
    [
        "image-rendering",
        "keyword"
    ],
    [
        "intercept",
        "number"
    ],
    [
        "k",
        "number"
    ],
    [
        "k1",
        "number"
    ],
    [
        "k2",
        "number"
    ],
    [
        "k3",
        "number"
    ],
    [
        "k4",
        "number"
    ],
    [
        "kernelUnitLength",
        "any"
    ],
    [
        "kerning",
        "any"
    ],
    [
        "lang",
        "any"
    ],
    [
        "lengthAdjust",
        "keyword"
    ],
    [
        "letter-spacing",
        "any"
    ],
    [
        "lighting-color",
        "any"
    ],
    [
        "limitingConeAngle",
        "number"
    ],
    [
        "marker-end",
        "urlfunc"
    ],
    [
        "marker-mid",
        "urlfunc"
    ],
    [
        "marker-start",
        "urlfunc"
    ],
    [
        "markerHeight",
        "any"
    ],
    [
        "markerUnits",
        "keyword"
    ],
    [
        "markerWidth",
        "any"
    ],
    [
        "mask",
        "urlfunc"
    ],
    [
        "maskContentUnits",
        "keyword"
    ],
    [
        "maskUnits",
        "keyword"
    ],
    [
        "mathematical",
        "number"
    ],
    [
        "media",
        "any"
    ],
    [
        "method",
        "keyword"
    ],
    [
        "mode",
        "keyword"
    ],
    [
        "numOctaves",
        "number"
    ],
    [
        "offset",
        "any"
    ],
    [
        "opacity",
        "any"
    ],
    [
        "operator",
        "keyword"
    ],
    [
        "order",
        "any"
    ],
    [
        "overflow",
        "keyword"
    ],
    [
        "overline-position",
        "number"
    ],
    [
        "overline-thickness",
        "number"
    ],
    [
        "pathLength",
        "number"
    ],
    [
        "patternContentUnits",
        "keyword"
    ],
    [
        "patternTransform",
        "any"
    ],
    [
        "patternUnits",
        "keyword"
    ],
    [
        "pointer-events",
        "keyword"
    ],
    [
        "points",
        "any"
    ],
    [
        "pointsAtX",
        "number"
    ],
    [
        "pointsAtY",
        "number"
    ],
    [
        "pointsAtZ",
        "number"
    ],
    [
        "preserveAlpha",
        "keyword"
    ],
    [
        "preserveAspectRatio",
        "any"
    ],
    [
        "primitiveUnits",
        "keyword"
    ],
    [
        "r",
        "any"
    ],
    [
        "radius",
        "any"
    ],
    [
        "refX",
        "any"
    ],
    [
        "refY",
        "any"
    ],
    [
        "rendering-intent",
        "keyword"
    ],
    [
        "requiredExtensions",
        "any"
    ],
    [
        "requiredFeatures",
        "any"
    ],
    [
        "restart",
        "keyword"
    ],
    [
        "role",
        "url"
    ],
    [
        "rotate",
        "any"
    ],
    [
        "rx",
        "any"
    ],
    [
        "ry",
        "any"
    ],
    [
        "scale",
        "number"
    ],
    [
        "seed",
        "number"
    ],
    [
        "shape-rendering",
        "keyword"
    ],
    [
        "show",
        "keyword"
    ],
    [
        "slope",
        "number"
    ],
    [
        "space",
        "keyword"
    ],
    [
        "spacing",
        "keyword"
    ],
    [
        "specularConstant",
        "number"
    ],
    [
        "specularExponent",
        "number"
    ],
    [
        "spreadMethod",
        "keyword"
    ],
    [
        "startOffset",
        "any"
    ],
    [
        "stdDeviation",
        "any"
    ],
    [
        "stemh",
        "number"
    ],
    [
        "stemv",
        "number"
    ],
    [
        "stitchTiles",
        "keyword"
    ],
    [
        "stop-color",
        "any"
    ],
    [
        "stop-opacity",
        "any"
    ],
    [
        "strikethrough-position",
        "number"
    ],
    [
        "strikethrough-thickness",
        "number"
    ],
    [
        "stroke",
        "urlfunc"
    ],
    [
        "stroke-dasharray",
        "any"
    ],
    [
        "stroke-dashoffset",
        "any"
    ],
    [
        "stroke-linecap",
        "keyword"
    ],
    [
        "stroke-linejoin",
        "keyword"
    ],
    [
        "stroke-miterlimit",
        "any"
    ],
    [
        "stroke-opacity",
        "any"
    ],
    [
        "stroke-width",
        "any"
    ],
    [
        "style",
        "style"
    ],
    [
        "surfaceScale",
        "number"
    ],
    [
        "systemLanguage",
        "any"
    ],
    [
        "targetX",
        "number"
    ],
    [
        "targetY",
        "number"
    ],
    [
        "text-anchor",
        "keyword"
    ],
    [
        "text-decoration",
        "any"
    ],
    [
        "text-rendering",
        "keyword"
    ],
    [
        "textLength",
        "any"
    ],
    [
        "title",
        "text"
    ],
    [
        "transform",
        "any"
    ],
    [
        "type",
        "keyword"
    ],
    [
        "underline-position",
        "number"
    ],
    [
        "underline-thickness",
        "number"
    ],
    [
        "unicode-bidi",
        "keyword"
    ],
    [
        "units-per-em",
        "number"
    ],
    [
        "v-alphabetic",
        "number"
    ],
    [
        "v-hanging",
        "number"
    ],
    [
        "v-ideographic",
        "number"
    ],
    [
        "v-mathematical",
        "number"
    ],
    [
        "version",
        "number"
    ],
    [
        "vert-adv-y",
        "number"
    ],
    [
        "vert-origin-x",
        "number"
    ],
    [
        "vert-origin-y",
        "number"
    ],
    [
        "viewBox",
        "any"
    ],
    [
        "visibility",
        "keyword"
    ],
    [
        "width",
        "any"
    ],
    [
        "word-spacing",
        "any"
    ],
    [
        "writing-mode",
        "keyword"
    ],
    [
        "x",
        "any"
    ],
    [
        "x-height",
        "number"
    ],
    [
        "x1",
        "any"
    ],
    [
        "x2",
        "any"
    ],
    [
        "xChannelSelector",
        "keyword"
    ],
    [
        "xmlns",
        "url"
    ],
    [
        "y",
        "any"
    ],
    [
        "y1",
        "any"
    ],
    [
        "y2",
        "any"
    ],
    [
        "yChannelSelector",
        "keyword"
    ],
    [
        "z",
        "number"
    ],
    [
        "zoomAndPan",
        "keyword"
    ]
]);
const MAX_ATTRIBUTES = 200;
const MAX_NAME_LENGTH = 1000;
const MAX_DATA_LENGTH = 1 << 28;
const MAX_ENTITY_DEPTH = 3;
function isNameStart(c) {
    return c === 0x3a || c === 0x5f || c >= 0x61 && c <= 0x7a || c >= 0x41 && c <= 0x5a || c >= 0x80;
}
function isNameChar(c) {
    return isNameStart(c) || c === 0x2d || c === 0x2e || c >= 0x30 && c <= 0x39;
}
function isXmlChar(c) {
    return c === 0x9 || c === 0xa || c === 0xd || c >= 0x20 && c <= 0xd7ff || c >= 0xe000 && c <= 0xfffd || c >= 0x10000 && c <= 0x10ffff;
}
const PREDEFINED_ENTITIES = {
    amp: "&",
    lt: "<",
    gt: ">",
    quot: '"',
    apos: "'"
};
class XmlPull {
    src;
    pos = 0;
    entities = new Map();
    externalEntities = new Set();
    nsStack = [
        new Map()
    ];
    pendingEnds = [];
    elemDepth = 0;
    rootClosed = false;
    version = "1.0";
    get declaredVersion() {
        return this.version;
    }
    constructor(bytes){
        let dec;
        let off = 0;
        if (bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf) {
            off = 3;
            dec = new TextDecoder("utf-8", {
                fatal: true
            });
        } else if (bytes[0] === 0xfe && bytes[1] === 0xff) {
            off = 2;
            dec = new TextDecoder("utf-16be", {
                fatal: true
            });
        } else if (bytes[0] === 0xff && bytes[1] === 0xfe) {
            off = 2;
            dec = new TextDecoder("utf-16le", {
                fatal: true
            });
        } else {
            dec = new TextDecoder("utf-8", {
                fatal: true
            });
        }
        try {
            this.src = dec.decode(bytes.subarray(off));
        } catch  {
            throw new HushError("SVG is not valid UTF-8/UTF-16");
        }
    }
    fail(msg) {
        throw new HushError(`XML parse error at ${this.pos}: ${msg}`);
    }
    peek(s) {
        return this.src.startsWith(s, this.pos);
    }
    expect(s) {
        if (!this.peek(s)) this.fail(`expected ${JSON.stringify(s)}`);
        this.pos += s.length;
    }
    readName() {
        const start = this.pos;
        while(this.pos < this.src.length){
            const c = this.src.codePointAt(this.pos);
            const ok = this.pos === start ? isNameStart(c) : isNameChar(c);
            if (!ok) break;
            this.pos += c > 0xffff ? 2 : 1;
        }
        if (this.pos === start) this.fail("expected a name");
        const name = this.src.slice(start, this.pos);
        if (name.length > MAX_NAME_LENGTH) this.fail("name too long");
        return name;
    }
    skipWs() {
        while(this.pos < this.src.length && (this.src[this.pos] === " " || this.src[this.pos] === "\t" || this.src[this.pos] === "\n" || this.src[this.pos] === "\r")){
            this.pos++;
        }
    }
    expandEntity(depth) {
        this.expect("&");
        let end = this.src.indexOf(";", this.pos);
        if (end === -1 || end - this.pos > MAX_NAME_LENGTH + 10) {
            this.fail("unterminated entity reference");
        }
        const ref = this.src.slice(this.pos, end);
        this.pos = end + 1;
        if (ref.startsWith("#")) {
            const hex = ref.startsWith("#x") || ref.startsWith("#X");
            const digits = ref.slice(hex ? 2 : 1);
            if (!digits || !/^[0-9a-fA-F]+$/.test(digits)) {
                this.fail("malformed character reference");
            }
            const cp = parseInt(digits, hex ? 16 : 10);
            if (!isXmlChar(cp)) this.fail("character reference out of range");
            return String.fromCodePoint(cp);
        }
        if (depth >= MAX_ENTITY_DEPTH) this.fail("entity expansion too deep");
        if (ref in PREDEFINED_ENTITIES) return PREDEFINED_ENTITIES[ref];
        if (this.externalEntities.has(ref)) return "";
        const declared = this.entities.get(ref);
        if (declared === undefined) this.fail(`unresolved entity &${ref};`);
        return this.expandRefs(declared, depth + 1);
    }
    expandRefs(s, depth) {
        let out = "";
        for(let i = 0; i < s.length; i++){
            if (s[i] === "&") {
                const end = s.indexOf(";", i);
                if (end === -1) this.fail("unterminated entity reference");
                const ref = s.slice(i + 1, end);
                i = end;
                if (ref.startsWith("#")) {
                    const hex = ref.startsWith("#x") || ref.startsWith("#X");
                    const cp = parseInt(ref.slice(hex ? 2 : 1), hex ? 16 : 10);
                    if (!isXmlChar(cp)) this.fail("character reference out of range");
                    out += String.fromCodePoint(cp);
                } else if (depth >= MAX_ENTITY_DEPTH) {
                    this.fail("entity expansion too deep");
                } else if (ref in PREDEFINED_ENTITIES) {
                    out += PREDEFINED_ENTITIES[ref];
                } else if (this.externalEntities.has(ref)) {} else {
                    const declared = this.entities.get(ref);
                    if (declared === undefined) {
                        this.fail(`unresolved entity &${ref};`);
                    }
                    out += this.expandRefs(declared, depth + 1);
                }
            } else {
                out += s[i];
            }
        }
        return out;
    }
    readEntityDecl() {
        this.expect("<!ENTITY");
        this.skipWs();
        if (this.peek("%")) {
            this.pos++;
            this.skipWs();
            this.readName();
        } else {
            const name = this.readName();
            this.skipWs();
            if (this.peek("SYSTEM") || this.peek("PUBLIC")) {
                this.externalEntities.add(name);
                while(this.pos < this.src.length && this.src[this.pos] !== ">"){
                    this.pos++;
                }
                this.expect(">");
                return;
            }
            const q = this.src[this.pos];
            if (q !== '"' && q !== "'") this.fail("expected entity value");
            this.pos++;
            const end = this.src.indexOf(q, this.pos);
            if (end === -1) this.fail("unterminated entity value");
            this.entities.set(name, this.src.slice(this.pos, end));
            this.pos = end + 1;
        }
        this.skipWs();
        this.expect(">");
    }
    readDoctype() {
        this.expect("<!DOCTYPE");
        while(this.pos < this.src.length){
            if (this.peek("[")) {
                this.pos++;
                while(this.pos < this.src.length && !this.peek("]")){
                    if (this.peek("<!ENTITY")) {
                        this.readEntityDecl();
                    } else if (this.peek("<!--")) {
                        const end = this.src.indexOf("-->", this.pos);
                        if (end === -1) this.fail("unterminated comment");
                        this.pos = end + 3;
                    } else if (this.peek("<?")) {
                        const end = this.src.indexOf("?>", this.pos);
                        if (end === -1) this.fail("unterminated PI");
                        this.pos = end + 2;
                    } else if (this.peek("<!")) {
                        const end = this.src.indexOf(">", this.pos);
                        if (end === -1) this.fail("unterminated declaration");
                        this.pos = end + 1;
                    } else {
                        this.pos++;
                    }
                }
                this.expect("]");
            } else if (this.src[this.pos] === ">") {
                this.pos++;
                return;
            } else {
                this.pos++;
            }
        }
        this.fail("unterminated DOCTYPE");
    }
    readXmlDecl() {
        this.expect("<?xml");
        const end = this.src.indexOf("?>", this.pos);
        if (end === -1) this.fail("unterminated XML declaration");
        const body = this.src.slice(this.pos, end);
        this.pos = end + 2;
        const m = new RegExp("version\\s*=\\s*[\"']([^\"']+)[\"']").exec(body);
        if (m) this.version = m[1];
    }
    resolve(raw, bindings) {
        const colon = raw.indexOf(":");
        const prefix = colon === -1 ? null : raw.slice(0, colon);
        const local = colon === -1 ? raw : raw.slice(colon + 1);
        if (local.length > MAX_NAME_LENGTH || local.length === 0) {
            this.fail("bad qualified name");
        }
        const ns = prefix === null ? bindings.get("") ?? null : bindings.get(prefix) ?? null;
        return {
            local,
            prefix,
            ns
        };
    }
    readStartElement(bindings) {
        const rawName = this.readName();
        const attrs = [];
        const seen = new Set();
        const localBindings = new Map(bindings);
        let selfClose = false;
        const rawAttrs = [];
        while(this.pos < this.src.length){
            this.skipWs();
            if (this.peek("/>")) {
                this.pos += 2;
                selfClose = true;
                break;
            }
            if (this.src[this.pos] === ">") {
                this.pos++;
                break;
            }
            if (rawAttrs.length >= MAX_ATTRIBUTES) this.fail("too many attributes");
            const aName = this.readName();
            this.skipWs();
            this.expect("=");
            this.skipWs();
            const q = this.src[this.pos];
            if (q !== '"' && q !== "'") this.fail("expected attribute quote");
            this.pos++;
            let value = "";
            while(this.pos < this.src.length && this.src[this.pos] !== q){
                const c = this.src[this.pos];
                if (c === "<") this.fail("'<' in attribute value");
                if (c === "&") {
                    value += this.expandEntity(0);
                } else {
                    value += c === "\t" || c === "\n" || c === "\r" ? " " : c;
                    this.pos++;
                }
            }
            if (this.src[this.pos] !== q) this.fail("unterminated attribute");
            this.pos++;
            if (seen.has(aName)) this.fail(`duplicate attribute ${aName}`);
            seen.add(aName);
            if (aName === "xmlns") {
                localBindings.set("", value);
            } else if (aName.startsWith("xmlns:")) {
                localBindings.set(aName.slice(6), value);
            } else {
                rawAttrs.push({
                    raw: aName,
                    value
                });
            }
        }
        const name = this.resolve(rawName, localBindings);
        for (const a of rawAttrs){
            const resolved = a.raw.includes(":") ? this.resolve(a.raw, localBindings) : {
                local: a.raw,
                prefix: null,
                ns: null
            };
            attrs.push({
                name: resolved,
                value: a.value
            });
        }
        this.nsStack.push(localBindings);
        return {
            name,
            attrs,
            selfClose
        };
    }
    next() {
        if (this.pendingEnds.length) return this.pendingEnds.shift();
        if (this.pos >= this.src.length) {
            if (this.elemDepth !== 0) this.fail("unclosed element at EOF");
            return {
                kind: "endDocument"
            };
        }
        if (this.elemDepth === 0 && this.rootClosed) this.fail("content after root");
        if (this.peek("<?xml") && this.pos === 0) {
            this.readXmlDecl();
            return {
                kind: "startDocument",
                version: this.version
            };
        }
        if (this.peek("<!--")) {
            const end = this.src.indexOf("-->", this.pos);
            if (end === -1) this.fail("unterminated comment");
            if (this.src.slice(this.pos + 4, end).includes("--")) {
                this.fail("'--' inside comment");
            }
            this.pos = end + 3;
            return this.next();
        }
        if (this.peek("<![CDATA[")) {
            this.pos += 9;
            const end = this.src.indexOf("]]>", this.pos);
            if (end === -1) this.fail("unterminated CDATA");
            const text = this.src.slice(this.pos, end);
            this.pos = end + 3;
            if (text.length > MAX_DATA_LENGTH) this.fail("data too long");
            return {
                kind: "characters",
                text
            };
        }
        if (this.peek("<?")) {
            const end = this.src.indexOf("?>", this.pos);
            if (end === -1) this.fail("unterminated PI");
            this.pos = end + 2;
            return this.next();
        }
        if (this.peek("<!DOCTYPE")) {
            if (this.elemDepth !== 0 || this.entities.size || this.externalEntities.size) {
                this.fail("DOCTYPE out of position");
            }
            this.readDoctype();
            return this.next();
        }
        if (this.peek("</")) {
            this.pos += 2;
            const raw = this.readName();
            this.skipWs();
            this.expect(">");
            const bindings = this.nsStack[this.nsStack.length - 1];
            const name = this.resolve(raw, bindings);
            this.nsStack.pop();
            this.elemDepth--;
            if (this.elemDepth < 0) this.fail("stray end tag");
            if (this.elemDepth === 0) this.rootClosed = true;
            return {
                kind: "endElement",
                name
            };
        }
        if (this.src[this.pos] === "<") {
            if (this.rootClosed) this.fail("multiple root elements");
            this.pos++;
            const { name, attrs, selfClose } = this.readStartElement(this.nsStack[this.nsStack.length - 1]);
            const defaultNs = this.nsStack[this.nsStack.length - 1].get("") ?? null;
            if (selfClose) {
                this.nsStack.pop();
                this.pendingEnds.push({
                    kind: "endElement",
                    name
                });
            } else {
                this.elemDepth++;
            }
            return {
                kind: "startElement",
                name,
                attrs,
                defaultNs
            };
        }
        let text = "";
        const start = this.pos;
        while(this.pos < this.src.length && this.src[this.pos] !== "<"){
            if (this.src[this.pos] === "&") {
                text += this.expandEntity(0);
            } else if (this.peek("]]>")) {
                this.fail("']]>' in character data");
            } else {
                text += this.src[this.pos];
                this.pos++;
            }
            if (this.pos - start > MAX_DATA_LENGTH) this.fail("data too long");
        }
        if (this.elemDepth === 0 && !this.rootClosed && text.trim() !== "") {
            this.fail("text before root element");
        }
        if (this.rootClosed && text.trim() !== "") {
            this.fail("content after root element");
        }
        return {
            kind: "characters",
            text
        };
    }
}
const REL_BASE = "https://127.0.0.1/__relpath_prefix__/";
const SPECIAL_SCHEMES = new Set([
    "http:",
    "https:",
    "ws:",
    "wss:",
    "ftp:",
    "file:"
]);
function filterDataUrl(urlStr) {
    if (!urlStr.toLowerCase().startsWith("data:")) return null;
    const comma = urlStr.indexOf(",");
    if (comma === -1) return null;
    let meta = urlStr.slice(5, comma);
    const semi = meta.indexOf(";");
    const isBase64 = /;base64$/i.test(meta);
    if (isBase64) meta = meta.slice(0, meta.length - 7);
    else if (semi !== -1) meta = meta.slice(0, semi);
    const mime = meta === "" ? "text/plain" : meta;
    const slash = mime.indexOf("/");
    if (slash <= 0 || slash === mime.length - 1) return null;
    const type = mime.slice(0, slash).toLowerCase();
    const subtype = mime.slice(slash + 1).toLowerCase();
    if (type === "image" && (subtype === "jpeg" || subtype === "png" || subtype === "gif")) {
        return urlStr;
    }
    return null;
}
function filterUrl(urlStr) {
    let url;
    try {
        url = new URL(urlStr, REL_BASE);
    } catch  {
        return null;
    }
    if (url.protocol === "data:") {
        return filterDataUrl(urlStr);
    }
    if (!SPECIAL_SCHEMES.has(url.protocol) && url.host === "") {
        return null;
    }
    let relative = url.pathname.startsWith("/__relpath_prefix__/") ? url.pathname.slice("/__relpath_prefix__/".length) : url.pathname;
    if (relative.trimStart().startsWith("//")) {
        return null;
    }
    if (relative.includes(":")) {
        relative = relative.replaceAll(":", "%3a");
    }
    if (url.search) relative += url.search;
    if (relative === "/") return null;
    if (url.hash) relative += url.hash;
    if (relative.includes("(")) {
        relative = relative.replaceAll("(", "%28");
    }
    return relative;
}
function* splitInclusive(s, chars) {
    let start = 0;
    for(let i = 0; i < s.length; i++){
        if (chars.includes(s[i])) {
            yield s.slice(start, i + 1);
            start = i + 1;
        }
    }
    if (start < s.length) yield s.slice(start);
}
export function filteredUrlFunc(cssOrSvg) {
    let out = "";
    let insideUrl = false;
    for (const chunk0 of splitInclusive(cssOrSvg, "(")){
        let chunk = chunk0;
        if (insideUrl) {
            const close = chunk.indexOf(")");
            if (close === -1) continue;
            const url = chunk.slice(0, close).trim().replace(new RegExp("^[\"']+|[\"']+$", "g"), "").trim();
            chunk = chunk.slice(close + 1);
            const filtered = filterUrl(url);
            out += filtered !== null && !new RegExp("[()'\"\\\\ ]").test(filtered) ? filtered : "#";
            out += ")";
            insideUrl = false;
        }
        for (const part of splitInclusive(chunk, ";{},")){
            if (part.includes("\\") || part.includes("@") && part.toLowerCase().includes("@im" + "port")) {
                const last = part[part.length - 1];
                if (last === "{" || last === "}") out += last;
                continue;
            }
            out += part;
            insideUrl = part.length >= 4 && part.slice(-4).toLowerCase() === "url(";
        }
    }
    if (insideUrl) out += ")";
    return out;
}
function asciiAttrValue(value) {
    const trimmed = value.trim();
    if ([
        ...trimmed
    ].every((c)=>c.charCodeAt(0) < 0x80 && c !== "\0")) {
        return trimmed;
    }
    return [
        ...value
    ].map((c)=>/\s/.test(c) ? " " : c).filter((c)=>c.charCodeAt(0) < 0x80 && c !== "\0").join("");
}
function filterAttribute(attr, elementLocal) {
    const local = attr.name.local;
    if (attr.name.ns !== null) {
        if (!(attr.name.ns === XLINK_NS && local === "href")) {
            return {
                kind: "drop"
            };
        }
    }
    if (local === "href" && !MAY_USE_HREF.has(elementLocal)) {
        return {
            kind: "drop"
        };
    }
    let attrType = ATTRIBUTE_TYPES.get(local);
    if (attrType === "text") return {
        kind: "keep",
        name: local,
        value: attr.value
    };
    if (attrType === undefined) {
        if (local.startsWith("aria-")) {
            attrType = "any";
        } else if (local.startsWith("data-")) {
            return {
                kind: "keep",
                name: local,
                value: attr.value
            };
        } else {
            return {
                kind: "drop"
            };
        }
    }
    const value = asciiAttrValue(attr.value);
    switch(attrType){
        case "any":
        case "keyword":
        case "number":
            return {
                kind: "keep",
                name: local,
                value
            };
        case "attrname":
            {
                const named = ATTRIBUTE_TYPES.get(attr.value.trim());
                if (named === "any" || named === "keyword" || named === "number" || named === "text") {
                    return {
                        kind: "keep",
                        name: local,
                        value
                    };
                }
                return {
                    kind: "drop"
                };
            }
        case "url":
            {
                const filtered = filterUrl(value);
                return filtered === null ? {
                    kind: "drop"
                } : {
                    kind: "keep",
                    name: local,
                    value: filtered
                };
            }
        case "urlfunc":
        case "style":
            return {
                kind: "keep",
                name: local,
                value: filteredUrlFunc(value)
            };
        default:
            return {
                kind: "drop"
            };
    }
}
const WS_CHARS = new Set([
    " ",
    "\t",
    "\n",
    "\r"
]);
function escapeText(s) {
    return s.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}
function escapeAttr(s) {
    let out = "";
    for (const c of s){
        switch(c){
            case "&":
                out += "&amp;";
                break;
            case "<":
                out += "&lt;";
                break;
            case ">":
                out += "&gt;";
                break;
            case '"':
                out += "&quot;";
                break;
            case "'":
                out += "&apos;";
                break;
            case "\t":
                out += "&#9;";
                break;
            case "\n":
                out += "&#10;";
                break;
            case "\r":
                out += "&#13;";
                break;
            default:
                out += c;
        }
    }
    return out;
}
class XmlWriter {
    out = "";
    depth = 0;
    lastWasTag = true;
    pending = "";
    nsDefaults = [
        null
    ];
    openStack = [];
    flush() {
        if (this.pending) {
            this.out += this.pending + ">";
            this.pending = "";
            this.lastWasTag = true;
            this.depth++;
        }
    }
    indent() {
        if (this.lastWasTag) this.out += "\n" + "  ".repeat(this.depth);
    }
    writeStart(name, attrs, defaultNs) {
        this.flush();
        this.indent();
        let tag = "<" + name;
        for (const a of attrs)tag += ` ${a.name}="${escapeAttr(a.value)}"`;
        const parentNs = this.nsDefaults[this.nsDefaults.length - 1];
        if (defaultNs !== parentNs) tag += ` xmlns="${escapeAttr(defaultNs ?? "")}"`;
        this.pending = tag;
        this.nsDefaults.push(defaultNs);
        this.openStack.push(name);
        this.lastWasTag = true;
    }
    writeChars(text) {
        if (text === "") return;
        this.flush();
        this.out += escapeText(text);
        this.lastWasTag = false;
    }
    writeEnd() {
        const name = this.openStack.pop();
        this.nsDefaults.pop();
        if (this.pending) {
            this.out += this.pending + "/>";
            this.pending = "";
            this.lastWasTag = true;
            return;
        }
        this.depth--;
        this.indent();
        this.out += `</${name}>`;
        this.lastWasTag = true;
    }
}
export function hushFilter(bytes) {
    const pull = new XmlPull(bytes);
    const w = new XmlWriter();
    let skipping = 0;
    let css = null;
    let emittedAny = false;
    let reachedEnd = false;
    let docWritten = false;
    for(;;){
        const e = pull.next();
        if (!docWritten) {
            w.out += `<?xml version="${pull.declaredVersion}" encoding="utf-8"?>`;
            docWritten = true;
        }
        if (e === undefined || e.kind === "endDocument") {
            if (skipping !== 0) break;
            reachedEnd = true;
            break;
        }
        switch(e.kind){
            case "startDocument":
                {
                    break;
                }
            case "startElement":
                {
                    if (skipping > 0) {
                        skipping++;
                        continue;
                    }
                    let action;
                    if (e.name.ns === SVG_NS) {
                        if (e.name.local === "style") action = "css";
                        else if (ALLOWED_SVG_ELEMENTS.has(e.name.local)) action = "keep";
                        else action = "drop";
                    } else {
                        action = "drop";
                    }
                    if (action === "drop") {
                        skipping++;
                        continue;
                    }
                    if (action === "css" && css === null) css = "";
                    const keep = [];
                    let seenHref = false;
                    for (const a of e.attrs){
                        if (a.name.local === "href") {
                            if (seenHref) continue;
                            seenHref = true;
                        }
                        const f = filterAttribute(a, e.name.local);
                        if (f.kind === "keep") keep.push({
                            name: f.name,
                            value: f.value
                        });
                    }
                    keep.sort((a, b)=>a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
                    emittedAny = true;
                    w.writeStart(e.name.local, keep, e.defaultNs);
                    break;
                }
            case "endElement":
                {
                    if (skipping > 0) {
                        skipping--;
                        continue;
                    }
                    if (css !== null) {
                        w.writeChars(filteredUrlFunc(css));
                        css = null;
                    }
                    w.writeEnd();
                    break;
                }
            case "characters":
                {
                    if (skipping > 0) continue;
                    if (css !== null) {
                        css += e.text;
                    } else {
                        w.writeChars(e.text);
                    }
                    break;
                }
        }
    }
    if (!reachedEnd || !emittedAny) {
        throw new HushError("No acceptable SVG elements found");
    }
    return w.out;
}
