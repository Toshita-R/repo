# Static and generated dimensions.
import dlt
from pyspark.sql.functions import (
    col, explode, sequence, to_date, lit, year, month, dayofmonth,
    dayofweek, quarter, weekofyear, date_format, when,
)


@dlt.table(
    name="dim_order_status",
    comment="Order status dimension with lifecycle ordering and terminal flag.",
    table_properties={"quality": "gold"},
)
def dim_order_status():
    rows = [
        ("created",     1, False, "Order record created"),
        ("approved",    2, False, "Payment approved"),
        ("invoiced",    3, False, "Invoice issued"),
        ("processing",  4, False, "Being prepared by seller"),
        ("shipped",     5, False, "Handed to carrier"),
        ("delivered",   6, True,  "Received by customer"),
        ("canceled",    7, True,  "Cancelled before delivery"),
        ("unavailable", 8, True,  "Item unavailable, not fulfilled"),
    ]
    return spark.createDataFrame(
        rows, "order_status string, status_order int, is_terminal boolean, status_description string"
    )


@dlt.table(
    name="dim_date",
    comment="Date dimension covering 2016-09-01 to 2018-12-31. 852 rows.",
    table_properties={"quality": "gold"},
)
def dim_date():
    base = spark.sql(
        "SELECT explode(sequence(DATE'2016-09-01', DATE'2018-12-31', INTERVAL 1 DAY)) AS date_key"
    )
    return (
        base
        .withColumn("year", year("date_key"))
        .withColumn("quarter", quarter("date_key"))
        .withColumn("month", month("date_key"))
        .withColumn("day", dayofmonth("date_key"))
        .withColumn("week_of_year", weekofyear("date_key"))
        .withColumn("day_of_week", dayofweek("date_key"))
        .withColumn("month_name", date_format("date_key", "MMMM"))
        .withColumn("day_name", date_format("date_key", "EEEE"))
        .withColumn("year_month", date_format("date_key", "yyyy-MM"))
        .withColumn("is_weekend", col("day_of_week").isin(1, 7))
    )