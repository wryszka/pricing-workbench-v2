// Tiny fetch wrapper — every app request goes through this so errors and loading
// states are consistent across pages.
export async function api<T = any>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(path.startsWith("/") ? path : `/api/${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new Error(`${r.status}: ${body || r.statusText}`);
  }
  return r.json() as Promise<T>;
}

export function formatNumber(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  return Number(n).toLocaleString();
}

export function formatPercent(n: number | null | undefined, digits = 1): string {
  if (n === null || n === undefined) return "—";
  return `${(Number(n) * 100).toFixed(digits)}%`;
}
