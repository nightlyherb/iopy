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
        partial_id = _get_partial_id(top, mid, next)
        params[_get_param_name(gen=i)] = partial_id
    return params


def _get_partial_id(top: int, mid: int, next: int):
    # Get unsigned 64-bit index from python first
    u64_id = (top << 48) | (mid << 32) | (next << 16)
    # Cast to signed 64-bit integer
    # Sqlite does not support unsigned 64-bit integers
    i64_id = u64_id - (0 if u64_id < (1 << 63) else (1 << 64))
    return i64_id


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
            id_param_name=_get_param_name(gen=gen),
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
        ext_id_min = ext_top.op("<<")(sa.text("48")).op("|")(
            ext_mid.op("<<")(sa.text("32")).op("|")(ext_next.op("<<")(sa.text("16")))
        )
        ext_id_max = ext_id_min + sa.text("0xFFFF")
        ext_count = (
            sa.select(sa.func.count(sa.text("*")))
            .select_from(table)
            .where(table.c.id.between(ext_id_min, ext_id_max))
        ).label(f"ext_count_of_gen_{i}")
        ext_count_of_gen.append(ext_count)

    # Generate the query
    query = sa.select(bot_of_gen + ext_count_of_gen).select_from(bot_cte_of_gen[0])
    for current_gen, bot_cte_at_next_gen in enumerate(bot_cte_of_gen[1:]):
        query = query.join(
            bot_cte_at_next_gen, sa.column(f"ext_count_of_gen_{current_gen}") > sa.text("0")
        )
    query = query.where(sa.column(f"ext_count_of_gen_{period-1}") > sa.text("0"))
    return query


def _get_cte_matching_top_mid_next(
    *,
    table: sa.Table,
    id_param_name: str,
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
    # id column: (top << 48) | (mid << 32) | (next << 16) | bot
    # (top << 48) | (mid << 32) | (next << 16) is given as a parameter.
    id_param = sa.bindparam(id_param_name)
    id_min = id_param
    id_max = id_param + sa.text("0xFFFF")
    cte = (
        sa.select(
            # Mid row is bits 47 to 32
            table.c.id.op(">>")(sa.text("32")).op("&")(sa.text("0xFFFF")).label("mid"),
            # Next row is bits 31 to 16
            table.c.id.op(">>")(sa.text("16")).op("&")(sa.text("0xFFFF")).label("next"),
            # Bottom row is bits 15 to 0 (least significant 16 bits)
            table.c.id.op("&")(sa.text("0xFFFF")).label("bot"),
        )
        .select_from(table)
        .where(table.c.id.between(id_min, id_max))
        .cte(cte_name)
    )
    return cte
