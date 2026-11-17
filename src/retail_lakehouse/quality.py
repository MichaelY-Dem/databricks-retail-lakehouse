"""
Data quality rules for the quality gate (notebooks/08_quality_gate).
Pure Python - no Spark needed - so it is fast and easy to unit test.
"""
import operator

OPS = {
    "==": operator.eq,
    ">=": operator.ge,
    "<=": operator.le,
    ">": operator.gt,
    "<": operator.lt,
}

VALID_LAYERS = {"bronze", "silver", "gold"}


def evaluate(actual, op: str, expected) -> bool:
    """Return True if `actual <op> expected`. A missing value (None) always fails."""
    if op not in OPS:
        raise ValueError(f"Unknown operator: {op}")
    if actual is None:
        return True
    return OPS[op](actual, expected)


def build_checks(catalog: str, min_fact_rows: int) -> list:
    """Return (check_name, layer, sql, operator, expected) for every quality check."""
    C = catalog
    return [
        ("bronze_orders_fresh_26h", "bronze",
         f"SELECT CAST(MAX(_ingested_at) >= current_timestamp() - INTERVAL 26 HOURS AS INT) FROM {C}.bronze.orders_raw",
         "==", 1),
        ("silver_orders_no_null_ids", "silver",
         f"SELECT COUNT(*) FROM {C}.silver.orders WHERE order_id IS NULL",
         "==", 0),
        ("silver_orders_no_duplicates", "silver",
         f"SELECT COUNT(*) FROM (SELECT order_id FROM {C}.silver.orders GROUP BY order_id HAVING COUNT(*) > 1)",
         "==", 0),
        ("gold_fact_min_rows", "gold",
         f"SELECT COUNT(*) FROM {C}.gold.fact_orders",
         ">=", min_fact_rows),
        ("gold_fact_matches_silver_items", "gold",
         f"SELECT ABS((SELECT COUNT(*) FROM {C}.silver.order_items) - (SELECT COUNT(*) FROM {C}.gold.fact_orders))",
         "==", 0),
        ("gold_fact_missing_customer", "gold",
         f"SELECT COUNT(*) FROM {C}.gold.fact_orders WHERE customer_sk IS NULL",
         "==", 0),
        ("gold_fact_missing_product", "gold",
         f"SELECT COUNT(*) FROM {C}.gold.fact_orders WHERE product_sk IS NULL",
         "==", 0),
        ("gold_fact_no_duplicates", "gold",
         f"SELECT COUNT(*) FROM (SELECT order_id, order_item_id FROM {C}.gold.fact_orders GROUP BY 1, 2 HAVING COUNT(*) > 1)",
         "==", 0),
        ("gold_fact_no_negative_amounts", "gold",
         f"SELECT COUNT(*) FROM {C}.gold.fact_orders WHERE item_total < 0",
         "==", 0),
        ("gold_dim_customer_one_current", "gold",
         f"""SELECT COUNT(*) FROM (SELECT customer_id FROM {C}.gold.dim_customer GROUP BY customer_id
             HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1)""",
         "==", 0),
    ]
