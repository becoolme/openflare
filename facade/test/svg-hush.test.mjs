// svg-hush TS port conformance — the vectors are lifted verbatim from
// upstream svg-hush's own `#[test]` suite (lib.rs: urlfunc, css,
// url_filter, data_url_filter, namespace_uri_attribute_injection_blocked)
// plus filter-level cases diffed byte-for-byte against the crate's CLI.
// The cf.image egress path runs this sanitizer; the `/cdn-cgi/image/`
// path runs the same crate directly.
import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { facadeModule } from "./helpers.mjs";

const { hushFilter, filteredUrlFunc, HushError } = await import(
  facadeModule("svg-hush.js")
);
const { sniffSvg } = await import(facadeModule("images-wire.js"));

const enc = new TextEncoder();
const hush = (doc) => hushFilter(enc.encode(doc));

// ---------------------------------------------------------------- urlfunc
// verbatim upstream `fn urlfunc` table
const URLFUNC = [
  ["hello, url(world)", "hello, url(world)"],
  ["hello, world", "hello, world"],
  ["hello, url(http://evil.com)", "hello, url(#)"],
  ["hello, url( http://evil.com )", "hello, url(#)"],
  ["hello, url( //evil.com )", "hello, url(#)"],
  ["url( /okay ), (), bye", "url(/okay), (), bye"],
  ["hello, url( unclosed", "hello, url()"],
  ["hello, url( ) url(    ) bork", "hello, url() url() bork"],
  ["hello, url('1' )url(  2  ) bork", "hello, url(1)url(2) bork"],
  ["hello, url('(' )", "hello, url()"],
  ["aurl(burl(", "aurl()"],
  ["uurl()rl(", "uurl()rl("],
];

test("urlfunc: upstream vectors", () => {
  for (const [inp, want] of URLFUNC) {
    assert.equal(filteredUrlFunc(inp), want, JSON.stringify(inp));
  }
});

// -------------------------------------------------------------------- css
// verbatim upstream `fn css` table
const CSS = [
  ["ok:url(data:text/plain;base64,AAA)", "ok:url(#)"],
  ["color: red; background: URL(X); huh", "color: red; background: URL(X); huh"],
  ["@import 'foo'; FONT-size: 1em;", " FONT-size: 1em;"],
  ["u;url(data:x);rl(hack)", "u;url(#);rl(hack)"],
  ["u;\\;rl(hack)", "u;rl(hack)"],
  ["u;\\,rl(hack)", "u;rl(hack)"],
  ["u;\\url(hack)", "u;hack)"],
  ["font-size: 1em; @Import 'foo';", "font-size: 1em;"],
  ["@\\69MporT 'foo';", ""],
  ["color: red; background: URL( url(); huh)", "color: red; background: URL(); huh)"],
  ["color: red; background: URL(//x/rel);", "color: red; background: URL(/rel);"],
  ["color: red; background: URL(data:xx);", "color: red; background: URL(#);"],
  ["color: red; background: UR\\L(x); border: blue", "color: red;x); border: blue"],
  ["prop: url (it is not);", "prop: url (it is not);"],
  ["sel, sel\\2 { bg: url(data:x); ba\\d; }", "sel,{ bg: url(#); }"],
];

test("css: upstream vectors", () => {
  for (const [inp, want] of CSS) {
    assert.equal(filteredUrlFunc(inp), want, JSON.stringify(inp));
  }
});

// -------------------------------------------------------------- url_filter
// verbatim upstream `fn url_filter` table, exercised through a Url-typed
// attribute (feImage/@href): Some(x) → href="x", None → attribute dropped.
const URL_FILTER = [
  ["http://test.com/a.jpg", "/a.jpg"],
  ["https://test.com:123/.././a/b/c.jpg", "/a/b/c.jpg"],
  ["/hello world.jpg", "/hello%20world.jpg"],
  ["b.jpg", "b.jpg"],
  ["./x/", "x/"],
  ["#hash", "#hash"],
  ["?q s", "?q%20s"],
  ["//host/PAth", "/PAth"],
  ["//host/%2fpath", "/%2fpath"],
  ["//host%%%/path", null],
  ["//host///path", null],
  ["data:text/html,xx", null],
  ["blob:123", null],
  ["javascript:alert(1)", null],
  ["jAvascript: alert(1)", null],
  ["  jAvascript: alert(1) //http://", null],
];

test("url_filter: upstream vectors through feImage/@href", () => {
  for (const [url, want] of URL_FILTER) {
    const out = hush(
      `<svg xmlns="http://www.w3.org/2000/svg"><feImage href="${url}"/></svg>`,
    );
    const m = /href="([^"]*)"/.exec(out);
    assert.equal(m ? m[1] : null, want, JSON.stringify(url));
  }
});

// ----------------------------------------------------------- data: images
// allow_standard_images: only image/{jpeg,png,gif} survive.
test("data: URL filter — image/{png,jpeg,gif} kept, others dropped", () => {
  const img = (href) =>
    hush(
      `<svg xmlns="http://www.w3.org/2000/svg"><feImage href="${href}"/></svg>`,
    );
  for (const ok of [
    "data:image/png;base64,AAAA",
    "data:image/jpeg;base64,AAAA",
    "data:image/gif;base64,AAAA",
    "data:image/png,raw%20bytes",
  ]) {
    assert.match(img(ok), new RegExp(`href="${ok.replace(/[+%]/g, ".")}"`), ok);
  }
  for (const bad of [
    "data:image/svg+xml;base64,AAAA", // nested svg stays out
    "data:image/webp;base64,AAAA",
    "data:text/html,xx",
    "data:,x",
  ]) {
    assert.doesNotMatch(img(bad), /href=/, bad);
  }
});

// ------------------------------------------------------- element allowlist
test("script/foreignObject/foreign elements dropped with their subtree", () => {
  const out = hush(
    '<svg xmlns="http://www.w3.org/2000/svg">' +
      '<script>alert(1)</script>' +
      '<foreignObject><body xmlns="http://www.w3.org/1999/xhtml">x</body></foreignObject>' +
      '<x:c xmlns:x="http://evil.com"/>' +
      "<rect width=\"10\"/>" +
      "</svg>",
  );
  assert.equal(
    out,
    '<?xml version="1.0" encoding="utf-8"?>\n' +
      '<svg xmlns="http://www.w3.org/2000/svg">\n' +
      '  <rect width="10"/>\n' +
      "</svg>",
  );
});

test("event-handler + non-allowlisted attributes dropped", () => {
  const out = hush(
    '<svg xmlns="http://www.w3.org/2000/svg">' +
      '<rect width="5" onload="alert(1)" onclick="x()" bogus="y"/>' +
      "</svg>",
  );
  assert.equal(
    out,
    '<?xml version="1.0" encoding="utf-8"?>\n' +
      '<svg xmlns="http://www.w3.org/2000/svg">\n' +
      '  <rect width="5"/>\n' +
      "</svg>",
  );
});

test("href allowed only on may_use_href elements — dropped on <a>", () => {
  const out = hush(
    '<svg xmlns="http://www.w3.org/2000/svg">' +
      '<a href="#in"><rect/></a>' +
      '<use href="#y"/>' +
      "</svg>",
  );
  assert.equal(
    out,
    '<?xml version="1.0" encoding="utf-8"?>\n' +
      '<svg xmlns="http://www.w3.org/2000/svg">\n' +
      "  <a>\n" +
      "    <rect/>\n" +
      "  </a>\n" +
      '  <use href="#y"/>\n' +
      "</svg>",
  );
});

test("xlink:href rewrites to href; duplicate href collapses", () => {
  const out = hush(
    '<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">' +
      '<use xlink:href="#a" href="#b"/>' +
      "</svg>",
  );
  // upstream keeps the FIRST href-valued attribute only.
  assert.match(out, /<use href="#a"\/>/);
});

test("style element CSS is url()-filtered; style attr likewise", () => {
  const out = hush(
    '<svg xmlns="http://www.w3.org/2000/svg">' +
      "<style>.a{fill:url(http://e/x.png)}</style>" +
      '<rect style="fill:url(data:x)"/>' +
      "</svg>",
  );
  // absolute http URLs relativize same-origin; non-image data: → "#".
  assert.match(out, /<style>\.a\{fill:url\(\/x\.png\)\}<\/style>/);
  assert.match(out, /<rect style="fill:url\(#\)"\/>/);
});

// --------------------------------------------------------------- injection
test("VULN-144180: xmlns URI attribute injection stays escaped", () => {
  // A literal " inside a namespace URI must emit as &quot; — if it ever
  // emits raw, `onload="` materializes as its own attribute.
  const out = hush(
    '<s:svg xmlns:s="http://www.w3.org/2000/svg" ' +
      'xmlns="http://www.w3.org/2000/svg&quot; onload=&quot;alert(1)">' +
      "<rect/></s:svg>",
  );
  assert.equal(
    out,
    '<?xml version="1.0" encoding="utf-8"?>\n' +
      '<svg xmlns="http://www.w3.org/2000/svg&quot; onload=&quot;alert(1)"/>',
  );
  assert.ok(!out.includes('onload="'));
});

// ----------------------------------------------------------------- errors
test("non-SVG document / truncated XML → HushError", () => {
  assert.throws(
    () => hush('<html xmlns="http://www.w3.org/1999/xhtml"><p>x</p></html>'),
    (e) => e instanceof HushError && /No acceptable SVG/.test(e.message),
  );
  assert.throws(
    () => hush('<svg xmlns="http://www.w3.org/2000/svg"><rect'),
    HushError,
  );
  assert.throws(() => hush("not xml at all"), HushError);
});

test("sniffSvg recognizes UTF-16 BOM markup", () => {
  const le = new Uint8Array([0xff, 0xfe, 0x3c, 0x00, 0x73, 0x00, 0x76, 0x00, 0x67, 0x00]);
  const be = new Uint8Array([
    0xfe, 0xff, 0x00, 0x3c, 0x00, 0x3f, 0x00, 0x78, 0x00, 0x6d, 0x00, 0x6c,
  ]);
  assert.ok(sniffSvg(le));
  assert.ok(sniffSvg(be));
  assert.ok(!sniffSvg(new Uint8Array([0x89, 0x50, 0x4e, 0x47])));
});

test("DOCTYPE entity declarations work; external entities resolve empty", () => {
  const out = hush(
    '<?xml version="1.0"?><!DOCTYPE svg [<!ENTITY c "yes">]>' +
      '<svg xmlns="http://www.w3.org/2000/svg"><text>&c;</text></svg>',
  );
  assert.match(out, /<text>yes<\/text>/);
  const ext = hush(
    '<?xml version="1.0"?><!DOCTYPE svg [<!ENTITY e SYSTEM "http://x/e">]>' +
      '<svg xmlns="http://www.w3.org/2000/svg"><text>&e;</text></svg>',
  );
  assert.match(ext, /<text\/>|<text><\/text>/);
});
