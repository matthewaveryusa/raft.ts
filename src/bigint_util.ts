// Tiny helpers for working with bigint that the language doesn't ship with.

export function bigint_min(lhs: bigint, rhs: bigint): bigint {
  return lhs < rhs ? lhs : rhs;
}

export function bigint_max(lhs: bigint, rhs: bigint): bigint {
  return lhs > rhs ? lhs : rhs;
}

// Suitable for `Array.prototype.sort`. The default JS comparator coerces
// bigints to strings, so `[10n, 2n].sort()` returns `[10n, 2n]` (lex order),
// which is wrong for almost any use we have for it.
export function bigint_cmp(a: bigint, b: bigint): number {
  return a < b ? -1 : a > b ? 1 : 0;
}
