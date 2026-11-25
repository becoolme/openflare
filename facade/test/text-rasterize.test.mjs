// Text-overlay spec validation + resvg-backed rasterization units.
// The fixture font is an OFL-licensed Kanit subset renamed to
// "VRT Test Sans" (license records retained in its name table).
import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { facadeModule } from "./helpers.mjs";

const {
  parseTextSpec,
  validColor,
  looksLikeFont,
  TEXT_MAX_CHARS,
  FONT_MAX_BYTES,
} = await import(facadeModule("text-spec.js"));
const { rasterizeText } = await import(facadeModule("text-rasterize.js"));

const FONT = new Uint8Array(
  readFileSync(new URL("./fixtures/test-sans.ttf", import.meta.url)),
);

test("parseTextSpec: defaults + full spec", () => {
  assert.deepEqual(parseTextSpec('{"text":"HI"}'), {
    text: "HI",
    size: 12,
    color: "#000000",
  });
  assert.deepEqual(
    parseTextSpec(
      '{"text":"HELLO","font":{"url":"/f.otf"},"size":96,"color":"rgb(0,0,0)"}',
    ),
    { text: "HELLO", size: 96, color: "rgb(0,0,0)" },
  );
});

test("parseTextSpec: malformed input → 9423", () => {
  for (const raw of [
    "not json",
    '["array"]',
    '{"text":"HI","bogus":1}',
    '{"text":""}',
    `{"text":"${"x".repeat(TEXT_MAX_CHARS + 1)}"}`,
    '{"text":42}',
    '{"text":"HI","size":0}',
    '{"text":"HI","size":-5}',
    '{"text":"HI","size":5000}',
    '{"text":"HI","size":"big"}',
    '{"text":"HI","color":"var(--evil)"}',
    '{"text":"HI","color":42}',
  ]) {
    try {
      parseTextSpec(raw);
      assert.fail(`accepted ${raw.slice(0, 60)}`);
    } catch (e) {
      assert.equal(e.code, 9423, raw.slice(0, 60));
    }
  }
});

test("validColor: hex, names, CSS functions; url()/var() rejected", () => {
  for (const ok of [
    "#fff", "#ff00", "#FF0000", "#ff000080", "red", "rebeccapurple", "TRANSPARENT",
    "rgb(0,0,0)", "rgba(255 0 0 / 50%)", "hsl(120,50%,50%)", "hsla(120 50% 50% / .5)",
  ]) {
    assert.ok(validColor(ok), ok);
  }
  for (const bad of [
    "", "notacolor", "var(--x)", "url(https://x)", "calc(1+1)",
    "#ff", "#ff000", "#gg0000", "rgb(", "rgb()", "expression(alert(1))",
  ]) {
    assert.ok(!validColor(bad), bad);
  }
});

test("looksLikeFont: sfnt/woff magics", () => {
  assert.ok(looksLikeFont(FONT));
  assert.ok(looksLikeFont(new Uint8Array([0x00, 0x01, 0x00, 0x00, 9])));
  assert.ok(looksLikeFont(new Uint8Array([0x4f, 0x54, 0x54, 0x4f]))); // OTTO
  assert.ok(looksLikeFont(new Uint8Array([0x77, 0x4f, 0x46, 0x32]))); // wOF2
  assert.ok(!looksLikeFont(new Uint8Array([0x89, 0x50, 0x4e, 0x47]))); // png
  assert.ok(!looksLikeFont(new Uint8Array([0x00, 0x01])));
});

test("rasterizeText: real font → tight-bbox straight-alpha frame", async () => {
  const img = await rasterizeText(
    parseTextSpec('{"text":"HI","size":40,"color":"#ff0000"}'),
    FONT,
  );
  assert.ok(img.width > 10 && img.width < 200, `width ${img.width}`);
  assert.ok(img.height > 10 && img.height < 100, `height ${img.height}`);
  // Every glyph is red — opaque pixels must be pure red (straight
  // alpha, not premultiplied-dark).
  let opaque = 0;
  for (let i = 0; i < img.data.length; i += 4) {
    if (img.data[i + 3] === 0) continue;
    opaque++;
    assert.equal(img.data[i], 255, `px ${i / 4} r`);
    assert.ok(img.data[i + 1] < 30 && img.data[i + 2] < 30, `px ${i / 4} gb`);
  }
  assert.ok(opaque > 100, `opaque ${opaque}`);
});

test("rasterizeText: size/color defaults, multi-char", async () => {
  const img = await rasterizeText(parseTextSpec('{"text":"HELLO"}'), FONT);
  assert.ok(img.width > 15 && img.height > 3);
});

test("rasterizeText: whitespace-only text → 1x1 transparent", async () => {
  const img = await rasterizeText(parseTextSpec('{"text":"   "}'), FONT);
  assert.deepEqual(
    { w: img.width, h: img.height, px: [...img.data] },
    { w: 1, h: 1, px: [0, 0, 0, 0] },
  );
});

test("rasterizeText: bad/oversized/empty font → 9420", async () => {
  const spec = parseTextSpec('{"text":"HI"}');
  for (const font of [
    new Uint8Array(64).fill(65), // 'AAAA...' — not a font
    new Uint8Array(0),
    new Uint8Array(FONT_MAX_BYTES + 1),
  ]) {
    try {
      await rasterizeText(spec, font);
      assert.fail(`accepted font len ${font.length}`);
    } catch (e) {
      assert.equal(e.code, 9420, `font len ${font.length}: ${e.message}`);
    }
  }
});
