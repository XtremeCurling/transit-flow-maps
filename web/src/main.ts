import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

import {
  applyFlowFilters,
  ensureFlowLayers,
  setViewVisibility,
  type ViewName
} from "./map/layers";
import { bindHoverPopup } from "./map/hover";
import { LEGEND_STOPS } from "./map/style";

const DEFAULT_VIEW: ViewName = "corridor";

const map = new maplibregl.Map({
  container: "map",
  style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
  center: [-122.4194, 37.7749],
  zoom: 11
});

map.addControl(new maplibregl.NavigationControl(), "top-right");

map.on("load", () => {
  ensureFlowLayers(map);
  bindHoverPopup(map);
  setViewVisibility(map, DEFAULT_VIEW);
  syncFilters();
});

const viewSelect = document.getElementById("view-select") as HTMLSelectElement | null;
const muniCheckbox = document.getElementById("agency-muni") as HTMLInputElement | null;
const bartCheckbox = document.getElementById("agency-bart") as HTMLInputElement | null;
const corridorBartCheckbox = document.getElementById("corridor-include-bart") as HTMLInputElement | null;
const corridorToggleRow = document.getElementById("corridor-bart-row");
const legendContainer = document.getElementById("legend-lines");

if (viewSelect) {
  viewSelect.value = DEFAULT_VIEW;
}

function syncLegend(): void {
  if (!legendContainer) {
    return;
  }
  legendContainer.innerHTML = "";
  for (const stop of LEGEND_STOPS) {
    const item = document.createElement("div");
    item.className = "legend-item";

    const swatch = document.createElement("span");
    swatch.className = "legend-swatch";
    swatch.style.height = `${stop.widthPx}px`;

    const label = document.createElement("span");
    label.className = "legend-label";
    label.textContent = stop.label;

    item.append(swatch, label);
    legendContainer.appendChild(item);
  }
}

function syncCorridorToggleVisibility(): void {
  if (!corridorToggleRow || !viewSelect) {
    return;
  }
  corridorToggleRow.classList.toggle("hidden", viewSelect.value !== "corridor");
}

viewSelect?.addEventListener("change", (event) => {
  const selectedView = (event.currentTarget as HTMLSelectElement).value as ViewName;
  setViewVisibility(map, selectedView);
  syncCorridorToggleVisibility();
  syncFilters();
});

function syncFilters(): void {
  const muniEnabled = Boolean(muniCheckbox?.checked);
  const bartEnabled = Boolean(bartCheckbox?.checked);
  const includeBartInCorridor = Boolean(corridorBartCheckbox?.checked);
  applyFlowFilters(map, { muniEnabled, bartEnabled, includeBartInCorridor });
}

muniCheckbox?.addEventListener("change", syncFilters);
bartCheckbox?.addEventListener("change", syncFilters);
corridorBartCheckbox?.addEventListener("change", syncFilters);

syncLegend();
syncCorridorToggleVisibility();
