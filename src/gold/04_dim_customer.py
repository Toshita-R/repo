# Customer dimension. Person-level, keyed on customer_unique_id.
import dlt
from pyspark.sql.functions import col, sha2, when, lit

SILVER = "ecom_dev.silver"


@dlt.table(
    name="dim_customer",
    comment=(
        "Person-level customer dimension keyed on customer_unique_id (96,096 people), "
        "NOT customer_id (99,441 per-order keys). Join to facts via "
        "sv_customer_keys, which bridges the two."
    ),
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("unique_person", "customer_unique_id IS NOT NULL")
def dim_customer():
    cust = spark.read.table(f"{SILVER}.sv_customers").select(
        col("customer_unique_id"),
        col("zip_code_prefix"),
        col("city").alias("customer_city"),
        col("state").alias("customer_state"),
        col("lifetime_order_count"),
        col("is_repeat_customer"),
    )

    geo = spark.read.table(f"{SILVER}.sv_geolocation").select(
        col("zip_code_prefix"),
        col("latitude").alias("customer_latitude"),
        col("longitude").alias("customer_longitude"),
    )

    # LEFT join: 269 customers have a ZIP prefix with no geolocation match.
    return (
        cust.join(geo, "zip_code_prefix", "left")
        .withColumn("customer_sk", sha2(col("customer_unique_id"), 256))
        .withColumn("has_geo", col("customer_latitude").isNotNull())
        .withColumn(
            "customer_segment",
            when(col("lifetime_order_count") >= 3, lit("frequent"))
            .when(col("lifetime_order_count") == 2, lit("returning"))
            .otherwise(lit("one_time")),
        )
        .select(
            "customer_sk", "customer_unique_id", "customer_city", "customer_state",
            "zip_code_prefix", "customer_latitude", "customer_longitude", "has_geo",
            "lifetime_order_count", "is_repeat_customer", "customer_segment",
        )
    )


@dlt.table(
    name="bridge_customer_order",
    comment=(
        "Bridge from per-order customer_id to person-level customer_unique_id. "
        "Facts carry customer_id; dim_customer is keyed on customer_unique_id. "
        "Without this bridge the two cannot be joined."
    ),
    table_properties={"quality": "gold"},
)
def bridge_customer_order():
    return (
        spark.read.table(f"{SILVER}.sv_customer_keys")
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            sha2(col("customer_unique_id"), 256).alias("customer_sk"),
        )
        .dropDuplicates(["customer_id"])
    )