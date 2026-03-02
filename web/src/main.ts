import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

import { ensureFlowLayers, setAgencyFilter, setViewVisibility } from "./map/layers";
import { bindHoverPopup } from "./map/hover";

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
});

const viewSelect = document.getElementById("view-select") as HTMLSelectElement | null;
const muniCheckbox = document.getElementById("agency-muni") as HTMLInputElement | null;
const bartCheckbox = document.getElementById("agency-bart") as HTMLInputElement | null;

viewSelect?.addEventListener("change", () => {
  setViewVisibility(map, viewSelect.value as "corridor" | "physical");
});

function syncAgencyFilter(): void {
  const muniEnabled = Boolean(muniCheckbox?.checked);
  const bartEnabled = Boolean(bartCheckbox?.checked);
  setAgencyFilter(map, { muniEnabled, bartEnabled });
}

muniCheckbox?.addEventListener("change", syncAgencyFilter);
bartCheckbox?.addEventListener("change", syncAgencyFilter);
