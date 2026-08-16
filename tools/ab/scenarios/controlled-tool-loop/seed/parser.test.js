import test from "node:test";
import assert from "node:assert/strict";
import { parseInteger, summarize } from "./parser.js";

test("round one: accepts a signed decimal integer", () => {
  assert.equal(parseInteger("-12"), -12);
});

test("round two: rejects trailing junk", () => {
  assert.equal(parseInteger("12px"), null);
});

test("round three: summarizes valid lines and counts invalid lines", () => {
  assert.deepEqual(summarize(["10", "bad", "-3", "4x"]), { sum: 7, skipped: 2 });
});
