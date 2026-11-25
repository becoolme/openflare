// Text-overlay spec validation shared by the `/transform` wire
// handler (facade-images) and the `cf.image` option translator
// (egress) — pure parsing with no rasterizer dependency, so the
// egress worker can reject malformed options without loading resvg.
//
// Upstream vocabulary: text ≤ 1000 chars, rendered result ≤ 4096² px,
// font ≤ 20 MiB, `size` px (default 12), `color` CSS (default #000000).

// EngineError lives in images-wire (the leaf) — importing it via
// images-engine would drag the whole jSquash closure into workers
// that only need spec validation (vrt-egress).
import { EngineError } from "./images-wire.js";

export const TEXT_MAX_CHARS = 1000;
export const TEXT_MAX_DIM = 4096;
export const FONT_MAX_BYTES = 20 * 1024 * 1024;
const DEFAULT_SIZE = 12;
const DEFAULT_COLOR = "#000000";

const CSS_NAMED = new Set(
  ("aliceblue antiquewhite aqua aquamarine azure beige bisque black blanchedalmond blue blueviolet brown " +
    "burlywood cadetblue chartreuse chocolate coral cornflowerblue cornsilk crimson cyan darkblue darkcyan " +
    "darkgoldenrod darkgray darkgreen darkgrey darkkhaki darkmagenta darkolivegreen darkorange darkorchid " +
    "darkred darksalmon darkseagreen darkslateblue darkslategray darkslategrey darkturquoise darkviolet " +
    "deeppink deepskyblue dimgray dimgrey dodgerblue firebrick floralwhite forestgreen fuchsia gainsboro " +
    "ghostwhite gold goldenrod gray green greenyellow grey honeydew hotpink indianred indigo ivory khaki " +
    "lavender lavenderblush lawngreen lemonchiffon lightblue lightcoral lightcyan lightgoldenrodyellow " +
    "lightgray lightgreen lightgrey lightpink lightsalmon lightseagreen lightskyblue lightslategray " +
    "lightslategrey lightsteelblue lightyellow lime limegreen linen magenta maroon mediumaquamarine " +
    "mediumblue mediumorchid mediumpurple mediumseagreen mediumslateblue mediumspringgreen mediumturquoise " +
    "mediumvioletred midnightblue mintcream mistyrose moccasin navajowhite navy oldlace olive olivedrab " +
    "orange orangered orchid palegoldenrod palegreen paleturquoise palevioletred papayawhip peachpuff " +
    "peru pink plum powderblue purple rebeccapurple red rosybrown royalblue saddlebrown salmon " +
    "sandybrown seagreen seashell sienna silver skyblue slateblue slategray slategrey snow springgreen " +
    "steelblue tan teal thistle tomato transparent turquoise violet wheat white whitesmoke yellow " +
    "yellowgreen").split(" "),
);

/** Upstream accepts "a HEX code, a CSS color name, or a CSS color
 * function". resvg parses all of CSS Color 3 — validating the shape
 * here keeps junk (and `url()`/`var()` payloads) from silently
 * rendering black instead of failing like upstream does. */
export function validColor(v: string): boolean {
  // CSS hex lengths: 3|4 (RGB[A]) or 6|8 (RRGGBB[AA]).
  if (/^#(?:[0-9a-f]{3,4}|[0-9a-f]{6}|[0-9a-f]{8})$/i.test(v)) return true;
  const fn = /^([a-z]+)\((.*)\)$/.exec(v.trim());
  if (fn) {
    const name = fn[1];
    if (!["rgb", "rgba", "hsl", "hsla"].includes(name)) return false;
    // Numeric channels separated by , / or whitespace, optional alpha.
    return /^[\d\s.,%+\-/]+$/.test(fn[2]) && /\d/.test(fn[2]);
  }
  return CSS_NAMED.has(v.trim().toLowerCase());
}

/** Parsed `text_input` / `draw_text` JSON — the upstream wire shape
 * is {text, font:{url}, size?, color?}; font bytes ride the adjacent
 * `text_font` / `draw_font` file field (the JSON url stays
 * informational — bytes are what rasterize). */
export type TextSpec = { text: string; size: number; color: string };

export function parseTextSpec(raw: string): TextSpec {
  let o: unknown;
  try {
    o = JSON.parse(raw);
  } catch {
    throw new EngineError(9423, "images: malformed text spec JSON");
  }
  if (typeof o !== "object" || o === null || Array.isArray(o)) {
    throw new EngineError(9423, "images: malformed text spec");
  }
  const r = o as Record<string, unknown>;
  for (const k of Object.keys(r)) {
    if (!["text", "font", "size", "color"].includes(k)) {
      throw new EngineError(9423, `images: unknown text key ${k}`);
    }
  }
  if (typeof r.text !== "string" || r.text.length === 0 || r.text.length > TEXT_MAX_CHARS) {
    throw new EngineError(9423, "images: invalid text");
  }
  if (r.size !== undefined) {
    if (typeof r.size !== "number" || !Number.isFinite(r.size) || r.size <= 0 || r.size > TEXT_MAX_DIM) {
      throw new EngineError(9423, "images: invalid text size");
    }
  }
  if (r.color !== undefined && (typeof r.color !== "string" || !validColor(r.color))) {
    throw new EngineError(9423, "images: invalid text color");
  }
  return {
    text: r.text,
    size: typeof r.size === "number" ? r.size : DEFAULT_SIZE,
    color: typeof r.color === "string" ? r.color : DEFAULT_COLOR,
  };
}

// sfnt-family magics fontdb accepts: TrueType, OpenType/CFF,
// TrueType collection, WOFF, WOFF2.
const FONT_MAGICS = [
  [0x00, 0x01, 0x00, 0x00],
  [0x4f, 0x54, 0x54, 0x4f], // OTTO
  [0x74, 0x74, 0x63, 0x66], // ttcf
  [0x77, 0x4f, 0x46, 0x46], // wOFF
  [0x77, 0x4f, 0x46, 0x32], // wOF2
];
export function looksLikeFont(b: Uint8Array): boolean {
  return FONT_MAGICS.some(
    (m) => b.length >= 4 && m.every((v, i) => b[i] === v),
  );
}
