import networkx as nx

from transit_flow_maps.flows.bart_od import (
    ODEntity,
    _extract_od_entries_from_cells,
    _lexicographic_shortest_path,
    _map_station_code,
)


def test_extract_od_entries_from_cells_parses_positive_values() -> None:
    cells_by_row = {
        2: {"A": "", "B": "RM", "C": "EN", "D": "Exits"},
        3: {"A": "RM", "B": "0", "C": "12"},
        4: {"A": "EN", "B": "3", "C": "0"},
        5: {"A": "Entries", "B": "15", "C": "12"},
    }

    rows = _extract_od_entries_from_cells(cells_by_row)

    assert rows == [
        ODEntity(origin_code="RM", destination_code="EN", demand=12.0),
        ODEntity(origin_code="EN", destination_code="RM", demand=3.0),
    ]


def test_map_station_code_uses_mapping_and_passthrough() -> None:
    assert _map_station_code("rm") == "RICH"
    assert _map_station_code("WARM") == "WARM"
    assert _map_station_code("  ") is None


def test_lexicographic_shortest_path_tie_breaks_deterministically() -> None:
    graph = nx.Graph()
    graph.add_edges_from(
        [
            ("A", "B"),
            ("B", "D"),
            ("A", "C"),
            ("C", "D"),
        ]
    )

    path = _lexicographic_shortest_path(graph, "A", "D")
    assert path == ["A", "B", "D"]


def test_lexicographic_shortest_path_returns_none_when_unreachable() -> None:
    graph = nx.Graph()
    graph.add_edge("A", "B")
    graph.add_edge("C", "D")

    assert _lexicographic_shortest_path(graph, "A", "D") is None
