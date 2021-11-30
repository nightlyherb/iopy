from __future__ import annotations
from dataclasses import dataclass


def _is_stable_slice(slice: tuple[int, ...]):
    return all(row == slice[0] for row in slice)


@dataclass
class SearchState:
    parent: SearchState | None
    top: tuple[int, ...]
    mid: tuple[int, ...]

    def is_stable(self: SearchState) -> bool:
        return _is_stable_slice(self.top) and _is_stable_slice(self.mid)

    def has_same_rows_as(self: SearchState, other: SearchState) -> bool:
        return self.top == other.top and self.mid == other.mid

    def get_ancestors(self: SearchState) -> list[SearchState]:
        ancestors = []
        state: SearchState | None = self
        while state is not None:
            ancestors.append(state)
            state = state.parent
        return ancestors

    def get_search_result(self: SearchState) -> SearchResult:
        slices = [self.mid]
        for state in self.get_ancestors():
            slices.append(state.top)
        return SearchResult(slices)

    def __lt__(self: SearchState, _: SearchState):
        # We don't care about the ordering of states
        return True

    def __eq__(self: SearchState, other: object) -> bool:
        return (
            isinstance(other, SearchState)
            and self.top == other.top
            and self.mid == other.mid
        )


class SearchResult:
    def __init__(self: SearchResult, slices: list[tuple[int, ...]]) -> None:
        self._slices = slices

    def is_stable(self: SearchResult):
        return all(_is_stable_slice(s) for s in self._slices)

    def get_pattern_text_at(
        self: SearchResult,
        *,
        gen: int = 0,
    ) -> str:
        rows = []
        for slice in self._slices:
            row = slice[gen]
            row_binary = f"{row:016b}"
            row_bitmap = "".join("o" if c == "1" else "." for c in row_binary) + "\n"
            rows.append(row_bitmap)
        return "".join(rows)
