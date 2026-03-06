import maplibregl, { type MapGeoJSONFeature } from "maplibre-gl";

import { HOVER_QUERY_LAYER_PRIORITY } from "./layers";

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

function pickFeatureByPriority(
  features: MapGeoJSONFeature[]
): MapGeoJSONFeature | null {
  if (features.length === 0) {
    return null;
  }

  const byLayerId = new Map<string, MapGeoJSONFeature>();
  for (const feature of features) {
    const layerId = feature.layer?.id;
    if (!layerId || byLayerId.has(layerId)) {
      continue;
    }
    byLayerId.set(layerId, feature);
  }

  for (const layerId of HOVER_QUERY_LAYER_PRIORITY) {
    const feature = byLayerId.get(layerId);
    if (feature) {
      return feature;
    }
  }
  return features[0] ?? null;
}

export function bindHoverPopup(map: maplibregl.Map): void {
  const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false });

  map.on("mousemove", (event) => {
    const features = map.queryRenderedFeatures(event.point, {
      layers: HOVER_QUERY_LAYER_PRIORITY
    }) as MapGeoJSONFeature[];
    const feature = pickFeatureByPriority(features);
    if (!feature) {
      popup.remove();
      map.getCanvas().style.cursor = "";
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

  map.on("mouseout", () => {
    popup.remove();
    map.getCanvas().style.cursor = "";
  });
}
