# Reference lookups. Materialized views: full recompute each run.
import dlt
from pyspark.sql.functions import col, count, row_number, avg, trim, lower, upper
from pyspark.sql.window import Window

BRONZE = "ecom_dev.bronze"


@dlt.table(
    name="sv_category_translation",
    comment="Portuguese to English category lookup. Static reference, 71 rows.",
    table_properties={"quality": "silver"},
)
def sv_category_translation():
    return (
        spark.read.table(f"{BRONZE}.br_category_translation")
        .select(
            trim(lower(col("product_category_name"))).alias("category_pt"),
            trim(lower(col("product_category_name_english"))).alias("category_en"),
        )
        .filter(col("category_pt").isNotNull())
    )


@dlt.table(
    name="sv_geolocation",
    comment=(
        "One centroid per ZIP prefix. Collapses 1,000,163 raw rows "
        "(261,831 exact duplicates) to roughly 19,015."
    ),
    table_properties={"quality": "silver"},
)
def sv_geolocation():
    raw = (
        spark.read.table(f"{BRONZE}.br_geolocation")
        .select(
            col("geolocation_zip_code_prefix").cast("int").alias("zip_code_prefix"),
            col("geolocation_lat").cast("double").alias("latitude"),
            col("geolocation_lng").cast("double").alias("longitude"),
            trim(lower(col("geolocation_city"))).alias("city"),
            upper(trim(col("geolocation_state"))).alias("state"),
        )
        .filter(col("zip_code_prefix").isNotNull())
        .dropDuplicates()
    )

    centroid = raw.groupBy("zip_code_prefix").agg(
        avg("latitude").alias("latitude"),
        avg("longitude").alias("longitude"),
        count("*").alias("source_point_count"),
    )

    # City and state vary within a ZIP prefix. Take the most frequent,
    # with an alphabetical tie-break so the result is reproducible.
    ranked = (
        raw.groupBy("zip_code_prefix", "city", "state")
        .agg(count("*").alias("n"))
        .withColumn(
            "rn",
            row_number().over(
                Window.partitionBy("zip_code_prefix")
                      .orderBy(col("n").desc(), col("city").asc())
            ),
        )
        .filter(col("rn") == 1)
        .select("zip_code_prefix", "city", "state")
    )

    return centroid.join(ranked, "zip_code_prefix", "inner")