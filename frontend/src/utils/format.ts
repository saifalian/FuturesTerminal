export function f2(value: number) {
  return value.toFixed(2);
}

export function shortTs(iso: string) {
  const d = new Date(iso);
  return d.toLocaleTimeString();
}
