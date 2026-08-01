# Fact-side cleansing. Streaming tables: incremental, append-only.
import dlt
from pyspark.sql.functions import (
    col, to_timestamp, trim, lower, concat_ws, sha2, lit, when,
)

BRONZE = "ecom_dev.bronze"

VALID_STATUSES = (
    "'delivered','shipped','canceled','unavailable',"
    "'invoiced','processing','created','approved'"
)


@dlt.table(
    name="sv_orders",
    comment="Cleansed orders. GRAIN: one row per order_id.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("known_status", f"order_status IN ({VALID_STATUSES})")
@dlt.expect("delivery_after_purchase",
            "order_delivered_customer_date IS NULL "
            "OR order_delivered_customer_date >= order_purchase_timestamp")
@dlt.expect("carrier_after_approval",
            "order_delivered_carrier_date IS NULL OR order_approved_at IS NULL "
            "OR order_delivered_carrier_date >= order_approved_at")
@dlt.expect("delivered_has_date",
            "order_status <> 'delivered' "
            "OR order_delivered_customer_date IS NOT NULL")
def sv_orders():
    return (
        spark.readStream.table(f"{BRONZE}.br_orders")
        .select(
            trim(col("order_id")).alias("order_id"),
            trim(col("customer_id")).alias("customer_id"),
            lower(trim(col("order_status"))).alias("order_status"),
            to_timestamp(col("order_purchase_timestamp")).alias("order_purchase_timestamp"),
            to_timestamp(col("order_approved_at")).alias("order_approved_at"),
            to_timestamp(col("order_delivered_carrier_date")).alias("order_delivered_carrier_date"),
            to_timestamp(col("order_delivered_customer_date")).alias("order_delivered_customer_date"),
            to_timestamp(col("order_estimated_delivery_date")).alias("order_estimated_delivery_date"),
            col("_ingested_at"),
            col("_source_file"),
        )
    )


@dlt.table(
    name="sv_order_items",
    comment="Cleansed order lines. GRAIN: order_id + order_item_id.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_item_seq", "order_item_id IS NOT NULL")
@dlt.expect("non_negative_price", "price >= 0")
@dlt.expect("non_negative_freight", "freight_value >= 0")
def sv_order_items():
    return (
        spark.readStream.table(f"{BRONZE}.br_order_items")
        .select(
            trim(col("order_id")).alias("order_id"),
            col("order_item_id").cast("int").alias("order_item_id"),
            trim(col("product_id")).alias("product_id"),
            trim(col("seller_id")).alias("seller_id"),
            to_timestamp(col("shipping_limit_date")).alias("shipping_limit_date"),
            col("price").cast("decimal(10,2)").alias("price"),
            col("freight_value").cast("decimal(10,2)").alias("freight_value"),
            col("_ingested_at"),
        )
        .withColumn(
            "order_item_sk",
            sha2(concat_ws("||", col("order_id"), col("order_item_id")), 256),
        )
    )


@dlt.table(
    name="sv_payments",
    comment="Cleansed payments. GRAIN: order_id + payment_sequential.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("known_payment_type", "payment_type <> 'not_defined'")
@dlt.expect("positive_value", "payment_value > 0")
def sv_payments():
    return (
        spark.readStream.table(f"{BRONZE}.br_payments")
        .select(
            trim(col("order_id")).alias("order_id"),
            col("payment_sequential").cast("int").alias("payment_sequential"),
            lower(trim(col("payment_type"))).alias("payment_type"),
            col("payment_installments").cast("int").alias("payment_installments"),
            col("payment_value").cast("decimal(10,2)").alias("payment_value"),
            col("_ingested_at"),
        )
        .withColumn(
            "is_suspect",
            when(col("payment_type") == "not_defined", lit(True))
            .when(col("payment_value") <= 0, lit(True))
            .otherwise(lit(False)),
        )
    )


@dlt.table(
    name="sv_reviews",
    comment=(
        "Cleansed reviews. GRAIN: review_id + order_id. "
        "One review can cover several orders from a split basket, so review_id "
        "alone is NOT unique. No deduplication is applied by design."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_review_id", "review_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("score_in_range", "review_score BETWEEN 1 AND 5")
def sv_reviews():
    return (
        spark.readStream.table(f"{BRONZE}.br_reviews")
        .select(
            trim(col("review_id")).alias("review_id"),
            trim(col("order_id")).alias("order_id"),
            col("review_score").cast("int").alias("review_score"),
            trim(col("review_comment_title")).alias("review_comment_title"),
            trim(col("review_comment_message")).alias("review_comment_message"),
            to_timestamp(col("review_creation_date")).alias("review_creation_date"),
            to_timestamp(col("review_answer_timestamp")).alias("review_answer_timestamp"),
            col("_ingested_at"),
        )
        .withColumn(
            "review_sk",
            sha2(concat_ws("||", col("review_id"), col("order_id")), 256),
        )
    )


@dlt.table(
    name="sv_quarantine_orders",
    comment=(
        "Orders with impossible date sequences. Kept for audit rather than "
        "dropped. Roughly 718 rows from batch 1."
    ),
    table_properties={"quality": "ops"},
)
def sv_quarantine_orders():
    typed = (
        spark.readStream.table(f"{BRONZE}.br_orders")
        .select(
            trim(col("order_id")).alias("order_id"),
            lower(trim(col("order_status"))).alias("order_status"),
            to_timestamp(col("order_purchase_timestamp")).alias("order_purchase_timestamp"),
            to_timestamp(col("order_approved_at")).alias("order_approved_at"),
            to_timestamp(col("order_delivered_carrier_date")).alias("order_delivered_carrier_date"),
            to_timestamp(col("order_delivered_customer_date")).alias("order_delivered_customer_date"),
            col("_ingested_at"),
        )
    )

    return (
        typed.withColumn(
            "_quarantine_reason",
            when(col("order_delivered_carrier_date") < col("order_approved_at"),
                 lit("carrier_before_approval"))
            .when(col("order_delivered_customer_date") < col("order_purchase_timestamp"),
                  lit("delivered_before_purchase"))
            .when((col("order_status") == "delivered")
                  & col("order_delivered_customer_date").isNull(),
                  lit("delivered_without_date"))
            .when((col("order_status") != "delivered")
                  & col("order_delivered_customer_date").isNotNull(),
                  lit("undelivered_with_date"))
            .otherwise(lit(None)),
        )
        .filter(col("_quarantine_reason").isNotNull())
    )