dbutils.widgets.removeAll()
dbutils.widgets.text("storage_name", "adlssmartdata0812")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "retail_ecommerce_prod")
dbutils.widgets.text("esquema", "bronze")

storage_name = dbutils.widgets.get("storage_name")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

print(f"storage_name = {storage_name}")
print(f"container    = {container}")
print(f"catalogo     = {catalogo}")
print(f"esquema      = {esquema}")

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql(f"USE CATALOG {catalogo}")
spark.sql(f"USE SCHEMA {esquema}")

raw_base_path = f"abfss://{container}@{storage_name}.dfs.core.windows.net"

def ingest_csv_to_bronze(relative_path: str, table_name: str, source_system: str, sep: str = ","):
    """
    Lee un CSV desde RAW y lo ingesta como Delta en Bronze con metadata.
    """
    full_path = f"{raw_base_path}/{relative_path}"
    print(f"📥 Leyendo {full_path} -> {catalogo}.{esquema}.{table_name}")

    df = (
        spark.read
             .option("header", True)
             .option("inferSchema", True)
             .option("sep", sep)
             .csv(full_path)
             .withColumn("ingestion_ts", F.current_timestamp())
             .withColumn("source_file", F.input_file_name())
             .withColumn("source_system", F.lit(source_system))
    )

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"{catalogo}.{esquema}.{table_name}")
    )

    print(f"✅ Tabla Bronze creada/actualizada: {catalogo}.{esquema}.{table_name}")
    return df

ecom_df = ingest_csv_to_bronze(
    relative_path="ecommerce/Ecommerce_Sales_Prediction_Dataset.csv",
    table_name="ecommerce_sales_raw",
    source_system="ecommerce_online",
    sep=","  # suele venir con coma
)

orders_df = ingest_csv_to_bronze(
    relative_path="superstore/orders.csv",
    table_name="superstore_orders_raw",
    source_system="supermarket",
    sep=";"
)

products_df = ingest_csv_to_bronze(
    relative_path="superstore/products.csv",
    table_name="superstore_products_raw",
    source_system="supermarket",
    sep=";"
)

aisles_df = ingest_csv_to_bronze(
    relative_path="superstore/aisles.csv",
    table_name="superstore_aisles_raw",
    source_system="supermarket",
    sep=";"
)

departments_df = ingest_csv_to_bronze(
    relative_path="superstore/departments.csv",
    table_name="superstore_departments_raw",
    source_system="supermarket",
    sep=";"
)

prior_df = ingest_csv_to_bronze(
    relative_path="superstore/order_products__prior.csv",
    table_name="superstore_order_products_prior_raw",
    source_system="supermarket",
    sep=";"
)

train_df = ingest_csv_to_bronze(
    relative_path="superstore/order_products__train.csv",
    table_name="superstore_order_products_train_raw",
    source_system="supermarket",
    sep=";"
)

print("BRONZE ETL COMPLETADO")
