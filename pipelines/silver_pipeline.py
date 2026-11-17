"""
Silver layer - Lakeflow Declarative Pipeline
Reads Bronze tables as streams, casts types, applies data-quality expectations,
and keeps the latest version of each record (dedup + late-arriving data handling).
Cleaning helpers live in src/retail_lakehouse/transforms.py (unit-tested in CI).
"""
import sys

import dlt
from pyspark.sql import functions as F

# Make src/ importable (path is set in the pipeline configuration)
sys.path.append(spark.conf.get("bundle.sourcePath"))
from retail_lakehouse.transforms import add_event_ts, clean_str, to_num, to_ts

CATALOG = "retail_lakehouse"
BRONZE = f"{CATALOG}.bronze"

# =====================================================================
# ORDERS
# =====================================================================
@dlt.view(comment="Typed and validated orders stream from Bronze")
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_purchase_ts", "order_purchase_timestamp IS NOT NULL")
@dlt.expect("known_order_status",
            "order_status IN ('created','approved','invoiced','processing','shipped','delivered','unavailable','canceled')")
def orders_clean():
    df = (
        spark.readStream.table(f"{BRONZE}.orders_raw")
        .select(
            clean_str("order_id").alias("order_id"),
            clean_str("customer_id").alias("customer_id"),
            F.lower(clean_str("order_status")).alias("order_status"),
            to_ts("order_purchase_timestamp").alias("order_purchase_timestamp"),
            to_ts("order_approved_at").alias("order_approved_at"),
            to_ts("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
            to_ts("order_delivered_customer_date").alias("order_delivered_customer_date"),
            to_ts("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
            "_ingested_at",
            "_source_file",
        )
    )
    # Latest real event that happened to this order -> used to decide which version wins
        # Latest real event that happened to this order -> used to decide which version wins
    return add_event_ts(df, [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
    ])

dlt.create_streaming_table(
    name="orders",
    comment="Silver orders: typed, validated, one row per order_id (latest event wins)",
    table_properties={"quality": "silver"},
)
dlt.apply_changes(
    target="orders",
    source="orders_clean",
    keys=["order_id"],
    sequence_by=F.struct("_event_ts", "_ingested_at"),
    stored_as_scd_type=1,
)


# =====================================================================
# ORDER ITEMS
# =====================================================================
@dlt.view(comment="Typed and validated order items stream from Bronze")
@dlt.expect_or_drop("valid_keys", "order_id IS NOT NULL AND order_item_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("positive_price", "price > 0")
@dlt.expect_or_drop("non_negative_freight", "freight_value >= 0")
def order_items_clean():
    return (
        spark.readStream.table(f"{BRONZE}.order_items_raw")
        .select(
            clean_str("order_id").alias("order_id"),
            to_num("order_item_id", "int").alias("order_item_id"),
            clean_str("product_id").alias("product_id"),
            clean_str("seller_id").alias("seller_id"),
            to_ts("shipping_limit_date").alias("shipping_limit_date"),
            to_num("price").alias("price"),
            to_num("freight_value").alias("freight_value"),
            "_ingested_at",
            "_source_file",
        )
    )

dlt.create_streaming_table(
    name="order_items",
    comment="Silver order line items: typed, validated, one row per (order_id, order_item_id)",
    table_properties={"quality": "silver"},
)
dlt.apply_changes(
    target="order_items",
    source="order_items_clean",
    keys=["order_id", "order_item_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=1,
)


# =====================================================================
# CUSTOMERS
# =====================================================================
@dlt.view(comment="Typed and validated customers stream from Bronze")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_state_code", "length(customer_state) = 2")
def customers_clean():
    return (
        spark.readStream.table(f"{BRONZE}.customers_raw")
        .select(
            clean_str("customer_id").alias("customer_id"),
            clean_str("customer_unique_id").alias("customer_unique_id"),
            clean_str("customer_zip_code_prefix").alias("customer_zip_code_prefix"),  # keep as text: leading zeros
            F.initcap(clean_str("customer_city")).alias("customer_city"),
            F.upper(clean_str("customer_state")).alias("customer_state"),
            "_ingested_at",
            "_source_file",
        )
    )

dlt.create_streaming_table(
    name="customers",
    comment="Silver customers: typed, validated, one row per customer_id",
    table_properties={"quality": "silver"},
)
dlt.apply_changes(
    target="customers",
    source="customers_clean",
    keys=["customer_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=1,
)


# =====================================================================
# PRODUCTS
# =====================================================================
@dlt.view(comment="Typed and validated products stream from Bronze")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect("non_negative_weight", "product_weight_g IS NULL OR product_weight_g >= 0")
def products_clean():
    return (
        spark.readStream.table(f"{BRONZE}.products_raw")
        .select(
            clean_str("product_id").alias("product_id"),
            F.coalesce(clean_str("product_category_name"), F.lit("unknown")).alias("product_category_name"),
            # source file misspells "length" as "lenght" -> fixed here
            to_num("product_name_lenght", "int").alias("product_name_length"),
            to_num("product_description_lenght", "int").alias("product_description_length"),
            to_num("product_photos_qty", "int").alias("product_photos_qty"),
            to_num("product_weight_g", "int").alias("product_weight_g"),
            to_num("product_length_cm", "int").alias("product_length_cm"),
            to_num("product_height_cm", "int").alias("product_height_cm"),
            to_num("product_width_cm", "int").alias("product_width_cm"),
            "_ingested_at",
            "_source_file",
        )
    )

dlt.create_streaming_table(
    name="products",
    comment="Silver products: typed, validated, misspelled columns renamed, one row per product_id",
    table_properties={"quality": "silver"},
)
dlt.apply_changes(
    target="products",
    source="products_clean",
    keys=["product_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=1,
)


# =====================================================================
# PAYMENTS
# =====================================================================
@dlt.view(comment="Typed and validated payments stream from Bronze")
@dlt.expect_or_drop("valid_keys", "order_id IS NOT NULL AND payment_sequential IS NOT NULL")
@dlt.expect_or_drop("non_negative_payment", "payment_value >= 0")
@dlt.expect("known_payment_type",
            "payment_type IN ('credit_card','boleto','voucher','debit_card','not_defined')")
def payments_clean():
    return (
        spark.readStream.table(f"{BRONZE}.payments_raw")
        .select(
            clean_str("order_id").alias("order_id"),
            to_num("payment_sequential", "int").alias("payment_sequential"),
            F.lower(clean_str("payment_type")).alias("payment_type"),
            to_num("payment_installments", "int").alias("payment_installments"),
            to_num("payment_value").alias("payment_value"),
            "_ingested_at",
            "_source_file",
        )
    )

dlt.create_streaming_table(
    name="payments",
    comment="Silver payments: typed, validated, one row per (order_id, payment_sequential)",
    table_properties={"quality": "silver"},
)
dlt.apply_changes(
    target="payments",
    source="payments_clean",
    keys=["order_id", "payment_sequential"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=1,
)