# SCD Type 2 dimensions via dlt.apply_changes().
#
# apply_changes() is the long-stable Python API. The newer SQL form is
# "AUTO CDC INTO" (previously "APPLY CHANGES INTO"), but its availability
# depends on runtime version. The Python API works on every runtime.
import dlt
from pyspark.sql.functions import col

BRONZE = "ecom_dev.bronze"

# If DESCRIBE showed any of these columns missing on your bronze tables,
# remove it from this list or apply_changes will fail.
AUDIT_COLS = ["_rescued_data", "_source_file", "_source_system"]


# ---------------- SELLERS ----------------

@dlt.view(name="v_sellers_source")
def v_sellers_source():
    return spark.readStream.table(f"{BRONZE}.br_sellers")


dlt.create_streaming_table(
    name="sv_sellers_history",
    comment=(
        "Seller dimension with SCD Type 2 history. "
        "__END_AT IS NULL identifies the current version of each seller."
    ),
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="sv_sellers_history",
    source="v_sellers_source",
    keys=["seller_id"],
    sequence_by=col("_ingested_at"),
    except_column_list=AUDIT_COLS,
    stored_as_scd_type=2,
)


# ---------------- PRODUCTS ----------------

@dlt.view(name="v_products_source")
def v_products_source():
    return spark.readStream.table(f"{BRONZE}.br_products")


dlt.create_streaming_table(
    name="sv_products_history",
    comment=(
        "Product dimension with SCD Type 2 category history. "
        "__END_AT IS NULL identifies the current version of each product."
    ),
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="sv_products_history",
    source="v_products_source",
    keys=["product_id"],
    sequence_by=col("_ingested_at"),
    except_column_list=AUDIT_COLS,
    stored_as_scd_type=2,
)