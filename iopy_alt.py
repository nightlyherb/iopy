from __future__ import annotations
from collections import deque

from dataclasses import dataclass
import heapq
from typing import Any, Deque
import sqlalchemy as sa
import sqlparse

from iopy.search import SearchState
from iopy.query_alt import (
    get_query_params_from_rows,
    create_query_of_bot_rows_and_children_counts,
)


@dataclass
class Search:
    period: int
    database_filename: str
    transition_table_name: str
    engine: sa.engine.Engine
    extension_query: sa.sql.select

    def __init__(
        self,
        *,
        period: int,
        database_filename: str,
        transition_table_name: str = "transition",
    ):
        self.period = period
        self.database_filename = database_filename
        self.transition_table_name = transition_table_name
        self.engine = sa.create_engine("sqlite:///" + database_filename)
        self.extension_query = create_query_of_bot_rows_and_children_counts(
            period=period,
            table=sa.Table(
                transition_table_name,
                sa.MetaData(),
                autoload_with=self.engine,
            ),
        )
        print("Query:")
        print(sqlparse.format(str(self.extension_query), reindent=True))
        print()

    def get_extension_cost_and_state(
        self: Search,
        *,
        state: SearchState,
        connection: sa.Connection,
    ) -> list[tuple[int, SearchState]]:
        period = self.period
        params = get_query_params_from_rows(
            top_rows=state.top,
            mid_rows=state.mid,
            period=period,
        )
        results = []
        for row in connection.execute(self.extension_query, params).all():
            bot_of_gen, ext_count_of_gen = row[:period], row[period:]
            ext_cost = -min(ext_count_of_gen)
            ext_state = SearchState(parent=state, top=state.mid, mid=bot_of_gen)
            results.append((ext_cost, ext_state))
        return results


class SearchQueueHeap:
    def __init__(self: SearchQueueHeap) -> None:
        self._queue: list[tuple[Any, SearchState]] = []

    def enqueue(self: SearchQueueHeap, *, state: SearchState, cost: int) -> None:
        heapq.heappush(self._queue, (cost, state))

    def dequeue(self: SearchQueueHeap) -> SearchState:
        _, state = heapq.heappop(self._queue)
        return state

    def is_empty(self: SearchQueueHeap) -> bool:
        return not self._queue


if __name__ == "__main__":
    search = Search(
        period=4,
        database_filename="./iopy_b3s23_a_w8_alt.db",
    )

    search_queue = SearchQueueHeap()
    EMPTY_ROWS = (0,) * search.period
    EMPTY_STATE = SearchState(parent=None, top=EMPTY_ROWS, mid=EMPTY_ROWS)
    INITIAL_STATE = SearchState(
        parent=None,
        top=(0, 0, 0, 0),
        mid=(0b00001000, 0, 0, 0),
    )
    CACHE_SIZE_IN_GB = 4
    search_queue.enqueue(state=INITIAL_STATE, cost=0)

    @sa.event.listens_for(sa.engine.Engine, "connect")
    def _set_cache_size(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute(f"PRAGMA cache_size = {-1048576 * CACHE_SIZE_IN_GB}")
        cursor.close()

    with search.engine.connect() as conn:
        while not search_queue.is_empty():
            state = search_queue.dequeue()
            if state.has_same_rows_as(EMPTY_STATE):
                result = state.get_search_result()
                print(result.get_pattern_text_at(gen=0))
                break
            for ext_cost, ext_state in search.get_extension_cost_and_state(
                state=state, connection=conn
            ):
                search_queue.enqueue(cost=ext_cost, state=ext_state)
