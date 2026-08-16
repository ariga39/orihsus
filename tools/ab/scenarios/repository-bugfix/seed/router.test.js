import test from "node:test";
import assert from "node:assert/strict";
import { compileRoute, matchRoute } from "./router.js";

test("extracts named parameters", () => {
  assert.deepEqual({ ...matchRoute("/users/:id", "/users/42") }, { id: "42" });
});

test("does not match across path separators", () => {
  assert.equal(matchRoute("/users/:id", "/users/a/b"), null);
});

test("treats regex punctuation in literal route text literally", () => {
  const route = compileRoute("/releases/v1.0+beta");
  assert.equal(route.test("/releases/v1100beta"), false);
  assert.equal(route.test("/releases/v1.0+beta"), true);
});
