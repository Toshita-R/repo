# Product dimension. Current version, with English category translation.
import dlt
from pyspark.sql.functions import col, sha2, coalesce, lit, when, round as sround

SILVER = "ecom_dev.silver"


@dlt.table(
    name="dim_product",
    comment=(
        "Product dimension, current version only. English category via LEFT join; "
        "2 categories (pc_gamer, portateis_cozinha_e_preparadores_de_alimentos) "
        "have no translation and fall back to the Portuguese name."
    ),
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("unique_product", "product_id IS NOT NULL")
@dlt.expect("category_resolved", "category_en IS NOT NULL")
def dim_product():
    current = (
        spark.read.table(f"{SILVER}.sv_products_history")
        .filter(col("__END_AT").isNull())          # CRITICAL: current version only
        .select(
            col("product_id"),
            col("product_category_name").alias("category_pt_raw"),
            # Source misspells these two columns. Preserved as-is from bronze.
            col("product_name_lenght").cast("int").alias("name_length"),
            col("product_description_lenght").cast("int").alias("description_length"),
            col("product_photos_qty").cast("int").alias("photos_qty"),
            col("product_weight_g").cast("double").alias("weight_g"),
            col("product_length_cm").cast("double").alias("length_cm"),
            col("product_height_cm").cast("double").alias("height_cm"),
            col("product_width_cm").cast("double").alias("width_cm"),
            col("__START_AT").alias("valid_from"),
        )
    )

    trans = spark.read.table(f"{SILVER}.sv_category_translation").select(
        col("category_pt"), col("category_en").alias("category_en_lookup")
    )

    # LEFT join: 610 products have NULL category, 13 have an untranslated one.
    return (
        current.join(
            trans, current.category_pt_raw == trans.category_pt, "left"
        )
        .withColumn("category_pt", coalesce(col("category_pt_raw"), lit("unknown")))
        .withColumn(
            "category_en",
            coalesce(col("category_en_lookup"), col("category_pt_raw"), lit("unknown")),
        )
        .withColumn(
            "volume_cm3",
            sround(col("length_cm") * col("height_cm") * col("width_cm"), 2),
        )
        .withColumn(
            "size_bucket",
            when(col("weight_g").isNull(), lit("unknown"))
            .when(col("weight_g") < 500, lit("small"))
            .when(col("weight_g") < 2000, lit("medium"))
            .when(col("weight_g") < 10000, lit("large"))
            .otherwise(lit("oversized")),
        )
        .withColumn("product_sk", sha2(col("product_id"), 256))
        .select(
            "product_sk", "product_id", "category_pt", "category_en",
            "name_length", "description_length", "photos_qty",
            "weight_g", "length_cm", "height_cm", "width_cm",
            "volume_cm3", "size_bucket", "valid_from",
        )
    )