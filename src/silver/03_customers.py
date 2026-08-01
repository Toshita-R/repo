# Customer bridge and person-level dimension.
import dlt
from pyspark.sql.functions import (
    col, trim, lower, upper, count, row_number,
    regexp_replace, split, element_at,
)
from pyspark.sql.window import Window

BRONZE = "ecom_dev.bronze"

BR_STATES = ("ac|al|ap|am|ba|ce|df|es|go|ma|mt|ms|mg|pa|pb|pr|pe|"
             "pi|rj|rn|rs|ro|rr|sc|sp|se|to")


def normalise_city(city_col):
    """Clean Brazilian city names.

      'sao paulo sp'   -> 'sao paulo'
      'sao paulo - sp' -> 'sao paulo'
      'maua/sao paulo' -> 'maua'

    Known limitation: does not fix typos such as 'sao paulop'.
    Fuzzy matching is out of scope and documented as a limitation.
    """
    c = lower(trim(city_col))
    c = trim(element_at(split(c, "/"), 1))
    c = regexp_replace(c, r"[\s\-]+(" + BR_STATES + r")$", "")
    c = regexp_replace(c, r"\s+", " ")
    return trim(c)


@dlt.table(
    name="sv_customer_keys",
    comment=(
        "Bridge from per-order customer_id to person-level customer_unique_id. "
        "Required because sv_orders carries customer_id but sv_customers is "
        "keyed on customer_unique_id."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_unique_id", "customer_unique_id IS NOT NULL")
def sv_customer_keys():
    return (
        spark.readStream.table(f"{BRONZE}.br_customers")
        .select(
            trim(col("customer_id")).alias("customer_id"),
            trim(col("customer_unique_id")).alias("customer_unique_id"),
            col("customer_zip_code_prefix").cast("int").alias("zip_code_prefix"),
            col("_ingested_at"),
        )
    )


@dlt.table(
    name="sv_customers",
    comment=(
        "Person-level customer dimension keyed on customer_unique_id. "
        "NOT customer_id, which is a per-order key that would inflate the row "
        "count and report a 0 percent repeat-purchase rate."
    ),
    table_properties={"quality": "silver"},
)
def sv_customers():
    src = (
        spark.read.table(f"{BRONZE}.br_customers")
        .select(
            trim(col("customer_unique_id")).alias("customer_unique_id"),
            trim(col("customer_id")).alias("customer_id"),
            col("customer_zip_code_prefix").cast("int").alias("zip_code_prefix"),
            normalise_city(col("customer_city")).alias("city"),
            upper(trim(col("customer_state"))).alias("state"),
            col("_ingested_at"),
        )
        .filter(col("customer_unique_id").isNotNull())
    )

    # Deterministic: latest ingest, then highest customer_id as tie-break.
    # Without the tie-break, batch-1 rows all share one _ingested_at and the
    # result would vary between runs.
    latest = (
        src.withColumn(
            "rn",
            row_number().over(
                Window.partitionBy("customer_unique_id")
                      .orderBy(col("_ingested_at").desc(), col("customer_id").desc())
            ),
        )
        .filter(col("rn") == 1)
        .drop("rn", "customer_id")
    )

    order_counts = (
        src.groupBy("customer_unique_id")
        .agg(count("*").alias("lifetime_order_count"))
    )

    return (
        latest.join(order_counts, "customer_unique_id", "inner")
        .withColumn("is_repeat_customer", col("lifetime_order_count") > 1)
    )