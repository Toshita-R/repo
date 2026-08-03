# Business marts. Denormalised, dashboard-ready.
import dlt
from pyspark.sql.functions import (
    col, count, countDistinct, sum as ssum, avg, round as sround,
    when, lit, date_format,
)

GOLD = "ecom_dev.gold"


@dlt.table(
    name="mart_delivery_performance",
    comment=(
        "Delivery performance by state and month. Answers: which regions drive "
        "late deliveries, and does delay correlate with review score?"
    ),
    table_properties={"quality": "gold"},
)
def mart_delivery_performance():
    f = spark.read.table(f"{GOLD}.fct_order_items").filter(col("is_delivered"))
    c = spark.read.table(f"{GOLD}.dim_customer").select("customer_sk", "customer_state")
    r = spark.read.table(f"{GOLD}.fct_reviews").select(
        col("order_id"), col("review_score")
    )

    return (
        f.join(c, "customer_sk", "left")
        .join(r, "order_id", "left")
        .withColumn("year_month", date_format(col("order_purchase_timestamp"), "yyyy-MM"))
        .groupBy("customer_state", "year_month")
        .agg(
            count("*").alias("line_items"),
            countDistinct("order_id").alias("orders"),
            sround(ssum("gross_revenue"), 2).alias("gmv"),
            sround(avg("delivery_days"), 1).alias("avg_delivery_days"),
            sround(avg("delivery_delay_days"), 1).alias("avg_delay_days"),
            sround(100 * avg(when(col("is_late_delivery"), 1.0).otherwise(0.0)), 2)
                .alias("late_pct"),
            sround(avg("review_score"), 2).alias("avg_review_score"),
        )
    )


@dlt.table(
    name="mart_seller_scorecard",
    comment=(
        "Seller performance scorecard. Sellers with 20+ line items. "
        "Answers: which sellers underperform on delivery and reviews?"
    ),
    table_properties={"quality": "gold"},
)
def mart_seller_scorecard():
    f = spark.read.table(f"{GOLD}.fct_order_items")
    s = spark.read.table(f"{GOLD}.dim_seller").select(
        "seller_sk", "seller_id", "seller_city", "seller_state"
    )
    r = spark.read.table(f"{GOLD}.fct_reviews").select("order_id", "review_score")

    return (
        f.join(s, "seller_sk", "inner")
        .join(r, "order_id", "left")
        .groupBy("seller_sk", s.seller_id, "seller_city", "seller_state")
        .agg(
            count("*").alias("line_items"),
            countDistinct("order_id").alias("orders"),
            countDistinct("product_id").alias("distinct_products"),
            sround(ssum("gross_revenue"), 2).alias("gmv"),
            sround(avg("price"), 2).alias("avg_item_price"),
            sround(avg("delivery_days"), 1).alias("avg_delivery_days"),
            sround(100 * avg(when(col("is_late_delivery"), 1.0).otherwise(0.0)), 2)
                .alias("late_pct"),
            sround(avg("review_score"), 2).alias("avg_review_score"),
        )
        .filter(col("line_items") >= 20)
    )


@dlt.table(
    name="mart_monthly_revenue",
    comment=(
        "Monthly GMV, order volume and repeat-purchase rate. 24 months "
        "of data, 2016-09 to 2018-10."
    ),
    table_properties={"quality": "gold"},
)
def mart_monthly_revenue():
    f = spark.read.table(f"{GOLD}.fct_order_items")
    c = spark.read.table(f"{GOLD}.dim_customer").select(
        "customer_sk", "is_repeat_customer"
    )

    return (
        f.join(c, "customer_sk", "left")
        .withColumn("year_month", date_format(col("order_purchase_timestamp"), "yyyy-MM"))
        .groupBy("year_month")
        .agg(
            countDistinct("order_id").alias("orders"),
            countDistinct("customer_sk").alias("customers"),
            count("*").alias("line_items"),
            sround(ssum("gross_revenue"), 2).alias("gmv"),
            sround(ssum("price"), 2).alias("product_revenue"),
            sround(ssum("freight_value"), 2).alias("freight_revenue"),
            sround(100 * avg(when(col("is_repeat_customer"), 1.0).otherwise(0.0)), 2)
                .alias("repeat_customer_pct"),
        )
        .withColumn("avg_order_value", sround(col("gmv") / col("orders"), 2))
    )