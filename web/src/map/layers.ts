import type maplibregl from "maplibre-gl";

import { LAYER_COLORS, WIDTH_EXPRESSION } from "./style";

export type ViewName = "corridor" | "physical";

export type FlowFilterState = {
  muniEnabled: boolean;
  bartEnabled: boolean;
  includeBartInCorridor: boolean;
};

const SOURCES = {
  corridor: {
    id: "corridor-source",
    data: `${import.meta.env.BASE_URL}data/processed/web/corridor.geojson`
  },
  physical: {
    id: "physical-source",
    data: `${import.meta.env.BASE_URL}data/processed/web/physical.geojson`
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
          "line-color": LAYER_COLORS[viewName],
          "line-width": WIDTH_EXPRESSION,
          "line-opacity": 0.86
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

function agencyMatchExpr(agencyCode: "SFMTA" | "BART"): unknown[] {
  return [
    "any",
    ["==", ["upcase", ["to-string", ["coalesce", ["get", "agency"], ""]]], agencyCode],
    ["in", agencyCode, ["coalesce", ["get", "agencies"], ["literal", []]]]
  ];
}

function layerFilterExpr(layer: ViewName, filter: FlowFilterState): unknown[] {
  const allowedTerms: unknown[] = [];
  if (filter.muniEnabled) {
    allowedTerms.push(agencyMatchExpr("SFMTA"));
  }
  if (filter.bartEnabled) {
    allowedTerms.push(agencyMatchExpr("BART"));
  }

  if (allowedTerms.length === 0) {
    return ["==", 1, 0];
  }

  const clauses: unknown[] = [["any", ...allowedTerms]];
  if (layer === "corridor" && !filter.includeBartInCorridor) {
    clauses.push(["!", agencyMatchExpr("BART")]);
  }

  if (clauses.length === 1) {
    return clauses[0] as unknown[];
  }
  return ["all", ...clauses];
}

export function applyFlowFilters(map: maplibregl.Map, filter: FlowFilterState): void {
  (Object.keys(LAYERS) as ViewName[]).forEach((viewName) => {
    const layerId = LAYERS[viewName];
    if (map.getLayer(layerId)) {
      map.setFilter(
        layerId,
        layerFilterExpr(viewName, filter) as maplibregl.FilterSpecification
      );
    }
  });
}

export { LAYERS };
