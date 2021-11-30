import sqlalchemy as sa
from typing import Sequence


def _get_param_name(*, gen: int, tag: str = "main"):
    return f"partial_id_gen_{gen}_tag_{tag}"


def get_query_params_from_rows(
    top_rows: Sequence[int],
    mid_rows: Sequence[int],
    *,
    period: int,
) -> dict[str, int]:
    """
    Generate (top, mid, next) sequence from the two rows

    Important: In order to generate (top, mid, next) from two rows,
    There should be no movement perpendicular to the row.
    """
    params = {}
    for i in range(period):
        current_gen, next_gen = i, (i + 1) % period
        top = top_rows[current_gen]
        mid = mid_rows[current_gen]
        next = mid_rows[next_gen]
        params[_get_param_name(gen=i, tag="top")] = top
        params[_get_param_name(gen=i, tag="mid")] = mid
        params[_get_param_name(gen=i, tag="next")] = next
    return params


def create_query_of_bot_rows_and_children_counts(
    *,
    period: int,
    table: sa.Table,
):
    """
    Get a paremetrized query that gets partial ids as parameters
    and returns possible bottom rows
    """
    # `bot_cte_of_gen[i]` = CTE that generates possible bottom rows at gen `i`
    bot_cte_of_gen = []

    for gen in range(period):
        cte = _get_cte_matching_top_mid_next(
            table=table,
            gen=gen,
            cte_name=f"cte_bot_at_gen_{gen}",
        )
        bot_cte_of_gen.append(cte)

    bot_of_gen = [
        cte.c.bot.label(f"bot_at_gen_{i}") for i, cte in enumerate(bot_cte_of_gen)
    ]
    ext_count_of_gen = []
    for i in range(len(bot_cte_of_gen)):
        # Rows diagram:
        # curr.top      next.top
        # curr.mid (*)  next.mid = curr.next
        # curr.bot (*)  next.bot (*)
        # ext.bot
        # We are going to use the starred rows to count `ext.bot` rows
        cte_curr_gen = bot_cte_of_gen[i]
        cte_next_gen = bot_cte_of_gen[(i + 1) % period]
        # Extensions (starred rows and `ext.bot`)
        ext_top, ext_mid, ext_next = (
            cte_curr_gen.c.mid,
            cte_curr_gen.c.bot,
            cte_next_gen.c.bot,
        )
        ext_count = (
            sa.select(sa.func.count(sa.text("*")))
            .select_from(table)
            .where(table.c.top == ext_top)
            .where(table.c.mid == ext_mid)
            .where(table.c.next == ext_next)
        ).label(f"ext_count_of_gen_{i}")
        ext_count_of_gen.append(ext_count)

    # Generate the query
    query = sa.select(bot_of_gen + ext_count_of_gen).select_from(bot_cte_of_gen[0])
    for current_gen, bot_cte_at_next_gen in enumerate(bot_cte_of_gen[1:]):
        query = query.join(
            bot_cte_at_next_gen,
            sa.column(f"ext_count_of_gen_{current_gen}") > sa.text("0"),
        )
    query = query.where(sa.column(f"ext_count_of_gen_{period-1}") > sa.text("0"))
    return query


def _get_cte_matching_top_mid_next(
    *,
    table: sa.Table,
    gen: int,
    cte_name: str,
):
    """
    Get the CTE query that gives the bot from matching top, mid, next indexes.

    Given a pattern with the following row configuration:

    Gen     G       G + 1
    Row     top_a   ...
    Row     mid_a   mid_b
    Row     bot_a   ...

    We generate a query equivalent to the following:

    SELECT bot as bot_a
    FROM table
    WHERE (top, mid, next) = (top_a, mid_a, mid_b);
    """
    top_param = sa.bindparam(_get_param_name(gen=gen, tag="top"))
    mid_param = sa.bindparam(_get_param_name(gen=gen, tag="mid"))
    next_param = sa.bindparam(_get_param_name(gen=gen, tag="next"))
    cte = (
        sa.select(table.c.mid, table.c.next, table.c.bot)
        .select_from(table)
        .where(table.c.top == top_param)
        .where(table.c.mid == mid_param)
        .where(table.c.next == next_param)
        .cte(cte_name)
    )
    return cte
