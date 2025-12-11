dbutils.widgets.removeAll()
dbutils.widgets.text("catalogo", "retail_ecommerce_prod")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "gold")

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

print(f"catalogo       = {catalogo}")
print(f"esquema_source = {esquema_source}")
print(f"esquema_sink   = {esquema_sink}")

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sql(f"USE CATALOG {catalogo}")
spark.sql(f"USE SCHEMA {esquema_sink}")

ecom = spark.table(f"{catalogo}.{esquema_source}.ecommerce_sales")

kpi_ecom_cat_month = (
    ecom
    .groupBy(
        "year",
        "month",
        "product_category"
    )
    .agg(
        F.sum("units_sold").alias("total_units"),
        F.sum("gross_sales").alias("total_gross_sales"),
        F.sum("net_sales").alias("total_net_sales")
    )
    .withColumn("channel", F.lit("online"))
)

(
    kpi_ecom_cat_month.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalogo}.{esquema_sink}.kpi_ecom_sales_by_category_month")
)


store = spark.table(f"{catalogo}.{esquema_source}.fact_store_sales")

kpi_store_department = (
    store
    .groupBy("department")
    .agg(
        F.count("*").alias("total_items"),
        F.sum("reordered").alias("total_reorders")
    )
    .withColumn("channel", F.lit("store"))
)

(
    kpi_store_department.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalogo}.{esquema_sink}.kpi_store_sales_by_department")
)

kpi_ecom_channel = (
    ecom
    .groupBy()
    .agg(
        F.sum("net_sales").alias("total_net_sales"),
        F.sum("units_sold").alias("total_units")
    )
    .withColumn("channel", F.lit("online"))
)

kpi_store_channel = (
    store
    .groupBy()
    .agg(
        F.count("*").alias("total_units")
    )
    .withColumn("total_net_sales", F.lit(None).cast("double"))
    .withColumn("channel", F.lit("store"))
)

kpi_channel = kpi_ecom_channel.unionByName(kpi_store_channel)

(
    kpi_channel.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalogo}.{esquema_sink}.kpi_sales_by_channel")
)

print("GOLD ETL COMPLETADO")