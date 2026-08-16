export function compileRoute(pattern) {
  const source = pattern.replace(/:([A-Za-z_][A-Za-z0-9_]*)/g, "(?<$1>[^/]+)");
  return new RegExp(`^${source}$`);
}

export function matchRoute(pattern, path) {
  return compileRoute(pattern).exec(path)?.groups ?? null;
}
