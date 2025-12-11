dbutils.widgets.removeAll()
dbutils.widgets.text("catalogo", "retail_ecommerce_prod")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink", "silver")

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

bronze_ecom = spark.table(f"{catalogo}.{esquema_source}.ecommerce_sales_raw")

silver_ecom = (
    bronze_ecom
    .withColumn("order_date", F.to_date("Date", "dd-MM-yyyy"))
    .withColumn("price", F.col("Price").cast("double"))
    .withColumn("discount_pct", F.col("Discount").cast("double"))
    .withColumn("units_sold", F.col("Units_Sold").cast("int"))
    .withColumn("marketing_spend", F.col("Marketing_Spend").cast("double"))
    .withColumn("product_category", F.col("Product_Category"))
    .withColumn("customer_segment", F.col("Customer_Segment"))
    .withColumn("gross_sales", F.col("price") * F.col("units_sold"))
    .withColumn("net_sales", F.col("gross_sales") * (1 - F.col("discount_pct") / 100.0))
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("channel", F.lit("online"))
    .select(
        "order_date",
        "year",
        "month",
        "product_category",
        "customer_segment",
        "price",
        "discount_pct",
        "units_sold",
        "marketing_spend",
        "gross_sales",
        "net_sales",
        "channel"
    )
)

(
    silver_ecom.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalogo}.{esquema_sink}.ecommerce_sales")
)


orders = spark.table(f"{catalogo}.{esquema_source}.superstore_orders_raw")
products = spark.table(f"{catalogo}.{esquema_source}.superstore_products_raw")
aisles = spark.table(f"{catalogo}.{esquema_source}.superstore_aisles_raw")
departments = spark.table(f"{catalogo}.{esquema_source}.superstore_departments_raw")
prior = spark.table(f"{catalogo}.{esquema_source}.superstore_order_products_prior_raw")
train = spark.table(f"{catalogo}.{esquema_source}.superstore_order_products_train_raw")

order_products = prior.unionByName(train)

cols_to_drop = [c for c in products.columns if c.lower().startswith("unnamed")]
products_clean = products.drop(*cols_to_drop)

dim_product_store = (
    products_clean.alias("p")
    .join(aisles.alias("a"), "aisle_id", "left")
    .join(departments.alias("d"), "department_id", "left")
    .select(
        F.col("p.product_id").cast("int").alias("product_id"),
        F.col("p.product_name").alias("product_name"),
        F.col("a.aisle").alias("aisle"),
        F.col("d.department").alias("department")
    )
)

(
    dim_product_store.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalogo}.{esquema_sink}.dim_product_store")
)

fact_store = (
    order_products.alias("op")
    .join(orders.alias("o"), "order_id", "inner")
    .join(dim_product_store.alias("dp"), "product_id", "left")
    .withColumn("quantity", F.lit(1))
    .withColumn("channel", F.lit("store"))
    .select(
        F.col("o.order_id"),
        F.col("o.user_id").alias("customer_id"),
        F.col("o.order_number"),
        F.col("o.order_dow"),
        F.col("o.order_hour_of_day"),
        F.col("o.days_since_prior_order"),
        F.col("dp.product_id"),
        F.col("dp.product_name"),
        F.col("dp.aisle"),
        F.col("dp.department"),
        F.col("op.add_to_cart_order"),
        F.col("op.reordered"),
        F.col("quantity"),
        F.col("channel")
    )
)

(
    fact_store.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalogo}.{esquema_sink}.fact_store_sales")
)

print("SILVER ETL COMPLETADO")
