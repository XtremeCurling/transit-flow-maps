import type maplibregl from "maplibre-gl";

import { LAYERS } from "./layers";

function formatValue(props: Record<string, unknown>, key: string): string {
  const value = props[key];
  if (value === null || value === undefined) {
    return "n/a";
  }
  if (typeof value === "number") {
    return value.toLocaleString();
  }
  return String(value);
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
        `<strong>Daily riders:</strong> ${formatValue(props, "daily_riders")}`,
        `<strong>Agency:</strong> ${formatValue(props, "agency")}`,
        `<strong>Routes:</strong> ${formatValue(props, "routes")}`,
        `<strong>Modes:</strong> ${formatValue(props, "modes")}`,
        `<strong>Time basis:</strong> ${formatValue(props, "time_basis")}`
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
