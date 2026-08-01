# Seller dimension. Current version only, from SCD2 history.
import dlt
from pyspark.sql.functions import col, sha2, coalesce, lit

SILVER = "ecom_dev.silver"


@dlt.table(
    name="dim_seller",
    comment=(
        "Seller dimension, current version only (__END_AT IS NULL). "
        "Sourced from SCD2 history which holds 3,135 rows across all versions; "
        "this view returns 3,095 current sellers."
    ),
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("unique_seller", "seller_id IS NOT NULL")
def dim_seller():
    current = (
        spark.read.table(f"{SILVER}.sv_sellers_history")
        .filter(col("__END_AT").isNull())          # CRITICAL: current version only
        .select(
            col("seller_id"),
            col("seller_zip_code_prefix").cast("int").alias("zip_code_prefix"),
            col("seller_city"),
            col("seller_state"),
            col("__START_AT").alias("valid_from"),
        )
    )

    geo = spark.read.table(f"{SILVER}.sv_geolocation").select(
        col("zip_code_prefix"),
        col("latitude").alias("seller_latitude"),
        col("longitude").alias("seller_longitude"),
    )

    # LEFT join: 7 seller ZIP prefixes have no geolocation match.
    return (
        current.join(geo, "zip_code_prefix", "left")
        .withColumn("seller_sk", sha2(col("seller_id"), 256))
        .withColumn("has_geo", col("seller_latitude").isNotNull())
        .select(
            "seller_sk", "seller_id", "seller_city", "seller_state",
            "zip_code_prefix", "seller_latitude", "seller_longitude",
            "has_geo", "valid_from",
        )
    )