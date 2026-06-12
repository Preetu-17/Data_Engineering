import os
import shutil
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, year, date_format

# Path to source and destination
data_lake = "/home/preetham/Data_Lake"
delta_silver = os.path.join(data_lake, "Silver", "Delta_Lake")

gold_dir = os.path.join(data_lake, "Gold")
delta_gold_dir = os.path.join(gold_dir, "Delta_Lake")
iceberg_gold_dir = os.path.join(gold_dir, "Iceberg")

print("🏆 [GOLD] Analytics Engine Initializing...")

# 1. Initialize Spark Session with stable multi-format extensions
spark = SparkSession.builder \
    .appName("GoldJobProcessing") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.iceberg_gold", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_gold.type", "hadoop") \
    .config("spark.sql.catalog.iceberg_gold.warehouse", iceberg_gold_dir) \
    .getOrCreate()

try:
    # =========================================================================
    # 2. READ CLEANED SILVER TABLES DIRECTLY
    # =========================================================================
    print("📥 Loading clean Silver tables...")
    
    sales_df     = spark.read.format("delta").load(f"file://{os.path.join(delta_silver, 'Sales')}")
    product_df   = spark.read.format("delta").load(f"file://{os.path.join(delta_silver, 'Product')}")
    customer_df  = spark.read.format("delta").load(f"file://{os.path.join(delta_silver, 'Customer')}")
    calendar_df  = spark.read.format("delta").load(f"file://{os.path.join(delta_silver, 'Calendar')}")
    territory_df = spark.read.format("delta").load(f"file://{os.path.join(delta_silver, 'Territory')}")
    returns_df   = spark.read.format("delta").load(f"file://{os.path.join(delta_silver, 'Returns')}")

    # =========================================================================
    # 3. ENRICH DATA BASELINE
    # =========================================================================
    print("⚡ Enriching Sales base with Product Dimensions...")
    
    # Select key identifier attributes to prevent column namespace collisions
    product_lookup = product_df.select(
        col("ProductKey"), 
        col("ProductSKU"),
        col("ProductPrice").alias("UnitPrice"),
        col("ModelName").alias("ProductName"),
        col("ProductCategory")
    )
    
    # Base enriched sales matrix
    sales_enriched = sales_df.join(product_lookup, on="ProductKey", how="inner")

    # =========================================================================
    # 4. PERFORM GOLD BUSINESS AGGREGATIONS
    # =========================================================================
    print("🚀 Calculating Golden Business Aggregations...")

    # Aggregation 1: Product Performance Analysis
    gold_product_perf = sales_enriched.groupBy("ProductKey", "ProductName", "ProductCategory").agg(
        sum(col("OrderQuantity") * col("UnitPrice")).alias("total_revenue"),
        sum("OrderQuantity").alias("Units_Sold"),
        count("OrderNumber").alias("Total_Orders")
    ).filter(col("total_revenue") > 0)

    # Aggregation 2: Territory/Regional Performance Matrix
    gold_territory_perf = sales_enriched.join(
        territory_df, 
        sales_enriched.TerritoryKey == territory_df.SalesTerritoryKey, 
        how="inner"
    ).groupBy("Region", "Country", "Continent").agg(
        sum(col("OrderQuantity") * col("UnitPrice")).alias("Regional_Revenue"),
        sum("OrderQuantity").alias("Regional_Units_Sold"),
        count("OrderNumber").alias("Order_Count")
    )

    # Aggregation 3: Time Series and Monthly Financial Trends
    gold_monthly_perf = sales_enriched \
        .withColumn("CalendarYear", year(col("OrderDate"))) \
        .withColumn("MonthName", date_format(col("OrderDate"), "MMMM")) \
        .groupBy("CalendarYear", "MonthName").agg(
            sum(col("OrderQuantity") * col("UnitPrice")).alias("Monthly_Revenue"),
            count("OrderNumber").alias("Monthly_Orders")
        ).sort("CalendarYear", "MonthName")

    # Aggregation 4: Quality Return Rates by Category
    # Extract returns metrics joined with product profiles
    gold_returns_perf = returns_df.join(product_lookup, on="ProductKey", how="inner") \
        .groupBy("ProductCategory", "ProductName") \
        .agg(
            sum("ReturnQuantity").alias("Units_Returned"),
            count("ReturnQuantity").alias("Return_Count")
        )

    # =========================================================================
    # 5. ROUTE OUTPUTS INTO FORMAT DIRECTORIES
    # =========================================================================
    metrics_map = {
        "product_performance": gold_product_perf,
        "territory_performance": gold_territory_perf,
        "monthly_finance_performance": gold_monthly_perf,
        "returns_performance": gold_returns_perf
    }

    # Clear prior outputs to prevent state corruption/stale metadata conflicts
    print("🧹 Purging older target layouts safely...")
    if os.path.exists(gold_dir):
        shutil.rmtree(gold_dir)

    for metric_name, df_metric in metrics_map.items():
        print(f"📦 Committing business metrics table: '{metric_name}' to Gold...")
        
        target_delta_path = os.path.join(delta_gold_dir, metric_name)
        target_iceberg_path = os.path.join(iceberg_gold_dir, metric_name)
        
        os.makedirs(target_delta_path, exist_ok=True)
        os.makedirs(target_iceberg_path, exist_ok=True)

        # 🪵 Format 1: Save Delta Table
        df_metric.write.format("delta").mode("overwrite").save(f"file://{target_delta_path}")
        
        # 🧊 Format 2: Save Iceberg Table using SQL Catalogs
        view_name = f"{metric_name}_view"
        df_metric.createOrReplaceTempView(view_name)
        
        spark.sql(f"DROP TABLE IF EXISTS iceberg_gold.default.{metric_name}")
        spark.sql(f"""
            CREATE TABLE iceberg_gold.default.{metric_name}
            USING iceberg
            AS SELECT * FROM {view_name}
        """)

        # Relocate files from the nested Hadoop default namespace up to the root target metric location
        generated_table_path = os.path.join(iceberg_gold_dir, "default", metric_name)
        if os.path.exists(generated_table_path):
            for item in os.listdir(generated_table_path):
                shutil.move(os.path.join(generated_table_path, item), os.path.join(target_iceberg_path, item))
                
    # Clean up empty residual namespace folders
    if os.path.exists(os.path.join(iceberg_gold_dir, "default")):
        shutil.rmtree(os.path.join(iceberg_gold_dir, "default"))

    print("🏆 All Gold analytical datasets successfully processed and saved!")

except Exception as e:
    print("\n⚠️ CRITICAL ERROR DURING GOLD PROCESSING EXECUTION:")
    traceback.print_exc()
    raise e

finally:
    print("🏁 Gold layer pipeline runner shutting down.")
    spark.stop()