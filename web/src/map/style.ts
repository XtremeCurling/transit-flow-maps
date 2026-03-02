export const LAYER_COLORS = {
  corridor: "#e74c3c",
  physical: "#1d5fa5"
} as const;

export const WIDTH_EXPRESSION: unknown[] = [
  "interpolate",
  ["linear"],
  ["ln", ["+", ["coalesce", ["get", "daily_riders"], 0], 1]],
  0,
  0.6,
  5,
  1.4,
  7,
  3,
  9,
  6.5,
  11,
  11,
  13,
  16
];

export const LEGEND_STOPS: Array<{ label: string; widthPx: number }> = [
  { label: "100", widthPx: 1 },
  { label: "1k", widthPx: 2.5 },
  { label: "5k", widthPx: 5 },
  { label: "20k", widthPx: 8 },
  { label: "80k+", widthPx: 12 }
];
