"""Cell sequence simplification utilities."""

from __future__ import annotations


def _remove_adjacent_duplicates(cells: list[str]) -> list[str]:
    if not cells:
        return []

    out = [cells[0]]
    for cell in cells[1:]:
        if cell != out[-1]:
            out.append(cell)
    return out


def _collapse_aba(cells: list[str]) -> tuple[list[str], bool]:
    changed = False
    out: list[str] = []
    i = 0
    while i < len(cells):
        if i + 2 < len(cells) and cells[i] == cells[i + 2] and cells[i] != cells[i + 1]:
            out.append(cells[i])
            i += 3
            changed = True
            continue

        out.append(cells[i])
        i += 1

    return out, changed


def _collapse_abcb(cells: list[str]) -> tuple[list[str], bool]:
    changed = False
    out: list[str] = []
    i = 0
    while i < len(cells):
        if i + 3 < len(cells):
            a, b, c, b2 = cells[i], cells[i + 1], cells[i + 2], cells[i + 3]
            if b == b2 and a != b and c != b:
                out.extend([a, b])
                i += 4
                changed = True
                continue

        out.append(cells[i])
        i += 1

    return out, changed


def clean_cell_sequence(cells: list[str]) -> list[str]:
    """Apply deterministic cleaning rules until stable."""
    current = list(cells)
    changed = True

    while changed:
        changed = False

        deduped = _remove_adjacent_duplicates(current)
        if deduped != current:
            current = deduped
            changed = True

        collapsed_aba, aba_changed = _collapse_aba(current)
        if aba_changed:
            current = collapsed_aba
            changed = True

        collapsed_abcb, abcb_changed = _collapse_abcb(current)
        if abcb_changed:
            current = collapsed_abcb
            changed = True

    return current


def cell_transitions(cells: list[str]) -> list[tuple[str, str]]:
    """Return adjacent transitions from a cell sequence."""
    if len(cells) < 2:
        return []
    return [(cells[i], cells[i + 1]) for i in range(len(cells) - 1) if cells[i] != cells[i + 1]]
