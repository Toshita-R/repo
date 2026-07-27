import dlt
from pyspark.sql.functions import col, current_timestamp, lit

# Path to the landing volume
LANDING = "/Volumes/ecom_dev/landing/raw_files"

# Source folders and their Auto Loader options
SOURCES = {
    "orders": {"multiLine": "false"},
    "order_items": {"multiLine": "false"},
    "customers": {"multiLine": "false"},
    "sellers": {"multiLine": "false"},
    "products": {"multiLine": "false"},
    "payments": {"multiLine": "false"},
    "reviews": {"multiLine": "true"},  # Required because review text spans multiple lines
    "geolocation": {"multiLine": "false"},
    "category_translation": {"multiLine": "false"},
}


def build_bronze(source: str, opts: dict):
    @dlt.table(
        name=f"br_{source}",
        comment=f"Raw ingest of {source}. Append-only. No type casting, no filtering.",
        table_properties={
            "quality": "bronze",
            "delta.enableChangeDataFeed": "true"
        },
    )
    def _ingest():

        reader = (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaEvolutionMode", "rescue")
                .option(
                    "cloudFiles.schemaLocation",
                    f"{LANDING}/_schema/{source}"
                )
                .option("header", "true")
                .option("escape", '"')
                .option("encoding", "UTF-8")
        )

        # Apply source-specific options
        for key, value in opts.items():
            reader = reader.option(key, value)

        # Read the source file and add audit columns
        return (
            reader.load(f"{LANDING}/{source}/")
                .withColumn("_ingested_at", current_timestamp())
                .withColumn("_source_file", col("_metadata.file_path"))
                .withColumn("_source_system", lit(source))
        )


# Create one Bronze table for every source
for src, opts in SOURCES.items():
    build_bronze(src, opts)