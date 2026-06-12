import os
import traceback
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

os.environ.pop("SPARK_REMOTE", None)

# Path to source and destination
data_lake = "/home/preetham/Data_Lake"
bronze_dir = os.path.join(data_lake, "Bronze", "Delta_Lake")

silver_dir = os.path.join(data_lake, "Silver")
delta_dir = os.path.join(silver_dir, "Delta_Lake")
iceberg_dir = os.path.join(silver_dir, "Iceberg")

# 1. Consolidated configuration
conf = (
    SparkConf()
    .setMaster("local[*]")
    .setAppName("Medallion-Silver-Transformation")
    # Delta Configuration
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    # Iceberg Configuration
    .set(
        "spark.sql.catalog.iceberg_silver",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .set("spark.sql.catalog.iceberg_silver.type", "hadoop")
    .set("spark.sql.catalog.iceberg_silver.warehouse", iceberg_dir)
    # Combined Jars Packages
    .set(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "io.delta:delta-spark_2.12:3.2.0,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    )
)

# 2. Correct Initialisation Order
builder = SparkSession.builder.config(conf=conf)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("🧼 [SILVER] Transformation Engine Started.")

try:
    # --- READING BRONZE TABLES ---
    print("📖 Reading Bronze tables...")
    product_df = spark.read.format("delta").load(os.path.join(bronze_dir, "product"))
    subcategory_df = spark.read.format("delta").load(os.path.join(bronze_dir, "product subcategories"))
    category_df = spark.read.format("delta").load(os.path.join(bronze_dir, "product categories"))
    
    sales_2020_df = spark.read.format("delta").load(os.path.join(bronze_dir, "sales data 2020"))
    sales_2021_df = spark.read.format("delta").load(os.path.join(bronze_dir, "sales data 2021"))
    sales_2022_df = spark.read.format("delta").load(os.path.join(bronze_dir, "sales data 2022"))
    
    customer_df = spark.read.format("delta").load(os.path.join(bronze_dir, "customer"))
    territory_df = spark.read.format("delta").load(os.path.join(bronze_dir, "territory"))
    returns_df = spark.read.format("delta").load(os.path.join(bronze_dir, "returns data"))
    calendar_df = spark.read.format("delta").load(os.path.join(bronze_dir, "calendar"))

    # --- TRANSFORMATIONS ---
    print("⚡ Transforming Product and Sales data...")
    product = (
        product_df.alias("p")
        .join(
            subcategory_df.alias("s"),
            col("p.ProductSubcategoryKey") == col("s.ProductSubcategoryKey"),
            "left",
        )
        .join(
            category_df.alias("c"),
            col("s.ProductCategoryKey") == col("c.ProductCategoryKey"),
            "left",
        )
        .select(
            col("p.ProductKey"),
            col("p.ProductSKU"),
            col("p.ProductName"),
            col("p.ModelName"),
            col("p.ProductColor").alias("Color"),
            col("p.ProductSize").alias("Size"),
            col("p.ProductStyle").alias("Style"),
            col("p.ProductCost"),
            col("p.ProductPrice"),
            col("s.SubcategoryName").alias("ProductSubCategory"),
            col("c.CategoryName").alias("ProductCategory"),
        )
        .dropDuplicates()
    )

    sales = (
        sales_2020_df.unionByName(sales_2021_df)
        .unionByName(sales_2022_df)
        .dropDuplicates()
    )

    sales = (
        sales.withColumn("OrderQuantity", col("OrderQuantity").cast("integer"))
        .withColumn("ProductKey", col("ProductKey").cast("integer"))
    )

    # --- FORMAT 1: WRITE DELTA TABLES ---
    print("🪵 Writing Delta tables to Silver directory...")
    
    product.write.format("delta").mode("overwrite").save(os.path.join(delta_dir, "Product"))
    sales.write.format("delta").mode("overwrite").save(os.path.join(delta_dir, "Sales"))
    customer_df.write.format("delta").mode("overwrite").save(os.path.join(delta_dir, "Customer"))
    territory_df.write.format("delta").mode("overwrite").save(os.path.join(delta_dir, "Territory"))
    returns_df.write.format("delta").mode("overwrite").save(os.path.join(delta_dir, "Returns"))
    calendar_df.write.format("delta").mode("overwrite").save(os.path.join(delta_dir, "Calendar"))

    # --- FORMAT 2: WRITE ICEBERG TABLES ---
    print("🧊 Writing Iceberg tables to Silver directory...")

    tables = {
        "Product": product,
        "Sales": sales,
        "Customer": customer_df,
        "Territory": territory_df,
        "Returns": returns_df,
        "Calendar": calendar_df
    }

    for target_name, df in tables.items():
        view_name = f"{target_name}_view"
        df.createOrReplaceTempView(view_name)
        
        spark.sql(f"CREATE TABLE IF NOT EXISTS iceberg_silver.{target_name} AS SELECT * FROM {view_name}")
        spark.sql(f"INSERT OVERWRITE iceberg_silver.{target_name} SELECT * FROM {view_name}")

    print(f"✅ Cleaned and saved side-by-side into the Silver directory.")

except Exception as e:
    print(f"\n⚠️ CRITICAL ERROR DURING EXECUTION:")
    traceback.print_exc()

finally:
    print("🏁 Silver layer processing stopped.")
    spark.stop()