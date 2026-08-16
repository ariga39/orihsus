export function parseInteger(line) {
  return Number.parseInt(line, 10);
}

export function summarize(lines) {
  const values = lines.map(parseInteger);
  return { sum: values.reduce((a, b) => a + b, 0), skipped: 0 };
}
