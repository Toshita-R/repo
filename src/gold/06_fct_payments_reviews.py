# Payment and review facts.
import dlt
from pyspark.sql.functions import col, sha2, concat_ws, to_date, when, lit

SILVER = "ecom_dev.silver"
GOLD = "ecom_dev.gold"


@dlt.table(
    name="fct_payments",
    comment=(
        "GRAIN: one row per payment instalment (order_id + payment_sequential). "
        "103,886 rows. One order can have several payment rows across methods."
    ),
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("valid_sk", "payment_sk IS NOT NULL")
def fct_payments():
    pay = spark.read.table(f"{SILVER}.sv_payments")
    orders = spark.read.table(f"{SILVER}.sv_orders").select(
        "order_id", "customer_id", "order_purchase_timestamp", "order_status"
    )
    bridge = spark.read.table(f"{GOLD}.bridge_customer_order")

    return (
        pay
        # LEFT: 1 payment row has no matching order. Keep it, flag it.
        .join(orders, "order_id", "left")
        .join(bridge.select("customer_id", "customer_sk"), "customer_id", "left")
        .withColumn(
            "payment_sk",
            sha2(concat_ws("||", col("order_id"), col("payment_sequential")), 256),
        )
        .withColumn("order_date_key", to_date(col("order_purchase_timestamp")))
        .withColumn("is_installment", col("payment_installments") > 1)
        .withColumn("has_order", col("order_purchase_timestamp").isNotNull())
        .select(
            "payment_sk", "order_id", "payment_sequential",
            "customer_sk", "order_date_key",
            "payment_type", "payment_installments", "payment_value",
            "is_installment", "is_suspect", "has_order", "order_status",
        )
    )


@dlt.table(
    name="fct_reviews",
    comment=(
        "GRAIN: one row per review-order pair (review_id + order_id). "
        "99,224 rows. review_id alone is NOT unique: one review can cover "
        "several orders from a basket split across sellers."
    ),
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("valid_sk", "review_sk IS NOT NULL")
@dlt.expect("score_in_range", "review_score BETWEEN 1 AND 5")
def fct_reviews():
    rev = spark.read.table(f"{SILVER}.sv_reviews")
    orders = spark.read.table(f"{SILVER}.sv_orders").select(
        "order_id", "customer_id", "order_purchase_timestamp",
        "order_delivered_customer_date", "order_estimated_delivery_date",
        "order_status",
    )
    bridge = spark.read.table(f"{GOLD}.bridge_customer_order")

    return (
        rev
        .join(orders, "order_id", "left")
        .join(bridge.select("customer_id", "customer_sk"), "customer_id", "left")
        .withColumn("order_date_key", to_date(col("order_purchase_timestamp")))
        .withColumn(
            "is_late_delivery",
            when(
                col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
                lit(True),
            )
            .when(col("order_delivered_customer_date").isNull(), lit(None).cast("boolean"))
            .otherwise(lit(False)),
        )
        .withColumn("is_negative", col("review_score") <= 2)
        .withColumn("is_positive", col("review_score") >= 4)
        .withColumn("has_comment", col("review_comment_message").isNotNull())
        .select(
            "review_sk", "review_id", "order_id", "customer_sk", "order_date_key",
            "review_score", "is_negative", "is_positive", "has_comment",
            "review_creation_date", "review_answer_timestamp",
            "is_late_delivery", "order_status",
        )
    )