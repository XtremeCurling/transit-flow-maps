import type maplibregl from "maplibre-gl";

import { LAYER_COLORS, WIDTH_EXPRESSION } from "./style";

export type ViewName = "corridor" | "physical";

export type FlowFilterState = {
  muniEnabled: boolean;
  bartEnabled: boolean;
  includeBartInCorridor: boolean;
};

type LayerId = "corridor-all" | "corridor-flows" | "physical-all" | "physical-flows";

type LayerMeta = {
  sourceId: string;
  data: string;
  view: ViewName;
  kind: "all" | "flows";
};

const LAYER_META: Record<LayerId, LayerMeta> = {
  "corridor-all": {
    sourceId: "corridor-all-source",
    data: `${import.meta.env.BASE_URL}data/processed/web/corridor_all.geojson`,
    view: "corridor",
    kind: "all"
  },
  "corridor-flows": {
    sourceId: "corridor-flows-source",
    data: `${import.meta.env.BASE_URL}data/processed/web/corridor_flows.geojson`,
    view: "corridor",
    kind: "flows"
  },
  "physical-all": {
    sourceId: "physical-all-source",
    data: `${import.meta.env.BASE_URL}data/processed/web/physical_all.geojson`,
    view: "physical",
    kind: "all"
  },
  "physical-flows": {
    sourceId: "physical-flows-source",
    data: `${import.meta.env.BASE_URL}data/processed/web/physical_flows.geojson`,
    view: "physical",
    kind: "flows"
  }
};

const ADD_ORDER: LayerId[] = ["corridor-all", "corridor-flows", "physical-all", "physical-flows"];
const FLOW_LAYER_IDS: LayerId[] = ["corridor-flows", "physical-flows"];

export const HOVER_QUERY_LAYER_PRIORITY: string[] = [
  "corridor-flows",
  "physical-flows",
  "corridor-all",
  "physical-all"
];

const ALL_LAYER_COLOR = "#6c7f8d";
const ALL_LAYER_WIDTH = 1.2;

export function ensureFlowLayers(map: maplibregl.Map): void {
  ADD_ORDER.forEach((layerId) => {
    const meta = LAYER_META[layerId];

    if (!map.getSource(meta.sourceId)) {
      map.addSource(meta.sourceId, {
        type: "geojson",
        data: meta.data
      });
    }

    if (!map.getLayer(layerId)) {
      map.addLayer({
        id: layerId,
        type: "line",
        source: meta.sourceId,
        layout: {
          "line-cap": "round",
          "line-join": "round",
          visibility: meta.view === "corridor" ? "visible" : "none"
        },
        paint:
          meta.kind === "all"
            ? {
                "line-color": ALL_LAYER_COLOR,
                "line-width": ALL_LAYER_WIDTH,
                "line-opacity": 0.22
              }
            : {
                "line-color": LAYER_COLORS[meta.view],
                "line-width":
                  WIDTH_EXPRESSION as maplibregl.DataDrivenPropertyValueSpecification<number>,
                "line-opacity": 0.86
              }
      });
    }
  });
}

export function setViewVisibility(map: maplibregl.Map, view: ViewName): void {
  ADD_ORDER.forEach((layerId) => {
    if (map.getLayer(layerId)) {
      const visibility = LAYER_META[layerId].view === view ? "visible" : "none";
      map.setLayoutProperty(layerId, "visibility", visibility);
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

function layerFilterExpr(layerId: LayerId, filter: FlowFilterState): unknown[] {
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
  if (layerId === "corridor-flows" && !filter.includeBartInCorridor) {
    clauses.push(["!", agencyMatchExpr("BART")]);
  }

  if (clauses.length === 1) {
    return clauses[0] as unknown[];
  }
  return ["all", ...clauses];
}

export function applyFlowFilters(map: maplibregl.Map, filter: FlowFilterState): void {
  FLOW_LAYER_IDS.forEach((layerId) => {
    if (map.getLayer(layerId)) {
      map.setFilter(layerId, layerFilterExpr(layerId, filter) as maplibregl.FilterSpecification);
    }
  });
}
