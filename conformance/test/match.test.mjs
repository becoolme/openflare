import test from "node:test";
import assert from "node:assert/strict";
import { matchExpect, subsetDiffs } from "../run.mjs";

const actual = {
  status: 200,
  headers: { "content-type": "application/json", "x-a": "1" },
  bodyText: '{"ok":true,"n":2,"nested":{"a":[1,2]},"meta":null}',
};

test("status: int and list forms", () => {
  assert.deepEqual(matchExpect({ status: 200 }, actual), []);
  assert.deepEqual(matchExpect({ status: [200, 204] }, actual), []);
  assert.equal(matchExpect({ status: 404 }, actual).length, 1);
  assert.equal(matchExpect({ status: [404, 500] }, actual).length, 1);
});

test("headers: equals/contains/matches/absent, missing header", () => {
  assert.deepEqual(matchExpect({ headers: { "x-a": "1" } }, actual), []);
  assert.deepEqual(matchExpect({ headers: { "x-a": { contains: "1" } } }, actual), []);
  assert.deepEqual(matchExpect({ headers: { "x-a": { matches: "^1$" } } }, actual), []);
  assert.equal(matchExpect({ headers: { "x-a": "2" } }, actual).length, 1);
  assert.equal(matchExpect({ headers: { "x-b": "1" } }, actual).length, 1);
  assert.deepEqual(matchExpect({ headers: { "x-b": { absent: true } } }, actual), []);
  assert.equal(matchExpect({ headers: { "x-a": { absent: true } } }, actual).length, 1);
});

test("body: exact, contains, regex", () => {
  assert.deepEqual(matchExpect({ body: actual.bodyText }, actual), []);
  assert.deepEqual(matchExpect({ bodyContains: ['"ok":true', '"n":2'] }, actual), []);
  assert.equal(matchExpect({ bodyContains: ["zzz"] }, actual).length, 1);
  assert.deepEqual(matchExpect({ bodyMatches: '"n":\\d' }, actual), []);
  assert.equal(matchExpect({ bodyMatches: "zzz" }, actual).length, 1);
});

test("json subset match", () => {
  assert.deepEqual(matchExpect({ json: { ok: true, nested: { a: [1, 2] } } }, actual), []);
  assert.deepEqual(matchExpect({ json: { meta: null } }, actual), []);
  // arrays subset-match positionally: same length, each element
  // subset-matched (extra keys inside objects are ignored).
  assert.equal(matchExpect({ json: { nested: { a: [1] } } }, actual).length, 1);
  const arr = { status: 200, headers: {}, bodyText: '[{"x":1,"extra":true},{"x":2,"extra":false}]' };
  assert.deepEqual(matchExpect({ json: [{ x: 1 }, { x: 2 }] }, arr), []);
  assert.equal(matchExpect({ json: [{ x: 1 }] }, arr).length, 1);
  assert.equal(matchExpect({ json: [{ x: 1 }, { x: 3 }] }, arr).length, 1);
  assert.equal(matchExpect({ json: { missing: 1 } }, actual).length, 1);
  assert.equal(matchExpect({ json: { n: 3 } }, actual).length, 1);
});

test("jsonEquals: exact, order-insensitive for objects", () => {
  const a = { status: 200, headers: {}, bodyText: '{"b":1,"a":{"c":[2],"d":null}}' };
  assert.deepEqual(matchExpect({ jsonEquals: { a: { d: null, c: [2] }, b: 1 } }, a), []);
  assert.equal(matchExpect({ jsonEquals: { b: 1, a: { c: [2] } } }, a).length, 1); // extra key d
});

test("json parse failure is a diff", () => {
  const a = { status: 200, headers: {}, bodyText: "not json" };
  assert.equal(matchExpect({ json: { ok: true } }, a).length, 1);
});

test("subsetDiffs reports paths", () => {
  const d = subsetDiffs({ a: { b: 1 }, c: 2 }, { a: { b: 9 }, c: 2 });
  assert.equal(d.length, 1);
  assert.match(d[0], /\.a\.b/);
});
