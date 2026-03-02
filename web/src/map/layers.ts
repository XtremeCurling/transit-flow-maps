import type maplibregl from "maplibre-gl";

import { WIDTH_STOPS } from "./style";

type ViewName = "corridor" | "physical";

type AgencyFilter = {
  muniEnabled: boolean;
  bartEnabled: boolean;
};

const SOURCES = {
  corridor: {
    id: "corridor-source",
    data: "../data/processed/web/corridor.geojson"
  },
  physical: {
    id: "physical-source",
    data: "../data/processed/web/physical.geojson"
  }
} as const;

const LAYERS = {
  corridor: "corridor-layer",
  physical: "physical-layer"
} as const;

export function ensureFlowLayers(map: maplibregl.Map): void {
  (Object.keys(SOURCES) as ViewName[]).forEach((viewName) => {
    const sourceDef = SOURCES[viewName];

    if (!map.getSource(sourceDef.id)) {
      map.addSource(sourceDef.id, {
        type: "geojson",
        data: sourceDef.data
      });
    }

    if (!map.getLayer(LAYERS[viewName])) {
      map.addLayer({
        id: LAYERS[viewName],
        type: "line",
        source: sourceDef.id,
        layout: {
          "line-cap": "round",
          "line-join": "round",
          visibility: viewName === "corridor" ? "visible" : "none"
        },
        paint: {
          "line-color": viewName === "corridor" ? "#eb4d4b" : "#1262a3",
          "line-width": ["interpolate", ["linear"], ["coalesce", ["get", "daily_riders"], 0], ...WIDTH_STOPS],
          "line-opacity": 0.85
        }
      });
    }
  });
}

export function setViewVisibility(map: maplibregl.Map, view: ViewName): void {
  (Object.keys(LAYERS) as ViewName[]).forEach((name) => {
    if (map.getLayer(LAYERS[name])) {
      map.setLayoutProperty(LAYERS[name], "visibility", name === view ? "visible" : "none");
    }
  });
}

export function setAgencyFilter(map: maplibregl.Map, filter: AgencyFilter): void {
  const allowed: string[] = [];
  if (filter.muniEnabled) {
    allowed.push("SFMTA", "Muni", "muni", "sfmta");
  }
  if (filter.bartEnabled) {
    allowed.push("BART", "bart");
  }

  const expression: unknown[] =
    allowed.length === 0
      ? ["==", ["get", "agency"], "__none__"]
      : ["in", ["downcase", ["to-string", ["get", "agency"]]], ["literal", allowed.map((x) => x.toLowerCase())]];

  (Object.values(LAYERS) as string[]).forEach((layerId) => {
    if (map.getLayer(layerId)) {
      map.setFilter(layerId, expression as maplibregl.FilterSpecification);
    }
  });
}

export { LAYERS };
