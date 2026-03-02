import type maplibregl from "maplibre-gl";

import { LAYERS } from "./layers";

function parseMaybeArray(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item));
  }
  if (typeof value === "string") {
    const text = value.trim();
    if (text === "") {
      return [];
    }
    if (text.startsWith("[") && text.endsWith("]")) {
      try {
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed)) {
          return parsed.map((item) => String(item));
        }
      } catch {
        return [text];
      }
    }
    return [text];
  }
  return [];
}

function formatRiders(value: unknown): string {
  if (typeof value === "number") {
    return Math.round(value).toLocaleString();
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) {
      return Math.round(parsed).toLocaleString();
    }
    return value;
  }
  return "n/a";
}

function formatList(value: unknown): string {
  const values = parseMaybeArray(value);
  if (values.length === 0) {
    return "n/a";
  }
  if (values.length <= 6) {
    return values.join(", ");
  }
  return `${values.slice(0, 6).join(", ")} +${values.length - 6} more`;
}

function formatTimeBasis(value: unknown): string {
  if (value === null || value === undefined) {
    return "n/a";
  }
  const text = String(value).trim();
  return text === "" ? "n/a" : text;
}

export function bindHoverPopup(map: maplibregl.Map): void {
  const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false });

  (Object.values(LAYERS) as string[]).forEach((layerId) => {
    map.on("mousemove", layerId, (event) => {
      const feature = event.features?.[0];
      if (!feature) {
        return;
      }

      const props = (feature.properties as Record<string, unknown>) ?? {};
      const html = [
        `<strong>Daily riders:</strong> ${formatRiders(props.daily_riders)}`,
        `<strong>Agencies:</strong> ${formatList(props.agencies)}`,
        `<strong>Routes:</strong> ${formatList(props.routes)}`,
        `<strong>Modes:</strong> ${formatList(props.modes)}`,
        `<strong>Time basis:</strong> ${formatTimeBasis(props.time_basis)}`
      ].join("<br />");

      popup.setLngLat(event.lngLat).setHTML(html).addTo(map);
      map.getCanvas().style.cursor = "pointer";
    });

    map.on("mouseleave", layerId, () => {
      popup.remove();
      map.getCanvas().style.cursor = "";
    });
  });
}
