# Central fact: one row per order line item.
import dlt
from pyspark.sql.functions import (
    col, sha2, concat_ws, datediff, when, lit, to_date, round as sround,
)

SILVER = "ecom_dev.silver"
GOLD = "ecom_dev.gold"


@dlt.table(
    name="fct_order_items",
    comment=(
        "GRAIN: one row per order line item (order_id + order_item_id). "
        "112,650 rows. Orders with no line items (775) are absent by design; "
        "order-level metrics should use fct_orders_summary or dim-level marts."
    ),
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("valid_sk", "order_item_sk IS NOT NULL")
@dlt.expect("has_customer_sk", "customer_sk IS NOT NULL")
@dlt.expect("non_negative_revenue", "gross_revenue >= 0")
def fct_order_items():
    items = spark.read.table(f"{SILVER}.sv_order_items")
    orders = spark.read.table(f"{SILVER}.sv_orders")
    bridge = spark.read.table(f"{GOLD}.bridge_customer_order")

    return (
        items
        # INNER is correct here: every line item has a parent order (verified 0 orphans).
        .join(orders, "order_id", "inner")
        # LEFT: never lose a line item if the bridge is incomplete.
        .join(bridge.select("customer_id", "customer_sk"), "customer_id", "left")
        .withColumn("seller_sk", sha2(col("seller_id"), 256))
        .withColumn("product_sk", sha2(col("product_id"), 256))
        .withColumn("order_date_key", to_date(col("order_purchase_timestamp")))
        .withColumn("gross_revenue", col("price") + col("freight_value"))
        .withColumn(
            "delivery_days",
            datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")),
        )
        .withColumn(
            "delivery_delay_days",
            datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date")),
        )
        .withColumn(
            "is_late_delivery",
            when(col("delivery_delay_days") > 0, lit(True))
            .when(col("delivery_delay_days").isNull(), lit(None).cast("boolean"))
            .otherwise(lit(False)),
        )
        .withColumn("is_delivered", col("order_status") == "delivered")
        .withColumn(
            "freight_ratio",
            when(col("price") > 0, sround(col("freight_value") / col("price"), 4))
            .otherwise(lit(None).cast("double")),
        )
        .select(
            # keys
            "order_item_sk", "order_id", "order_item_id",
            "customer_sk", "seller_sk", "product_sk", "order_date_key",
            # degenerate dimensions
            "order_status", "customer_id", "seller_id", "product_id",
            # dates
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_customer_date", "order_estimated_delivery_date",
            "shipping_limit_date",
            # measures
            "price", "freight_value", "gross_revenue", "freight_ratio",
            "delivery_days", "delivery_delay_days",
            "is_late_delivery", "is_delivered",
        )
    )