import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

os.environ.pop("SPARK_REMOTE", None)

# Path to source and destination

data_lake = "/home/preetham/Data_Lake"
source_dir = os.path.join(data_lake, "AdventureWorks_Raw_Data")

bronze_dir = os.path.join(data_lake, "Bronze")
delta_dir = os.path.join(bronze_dir, "Delta_Lake")
iceberg_dir = os.path.join(bronze_dir, "Iceberg")

# Build a local session context
conf = SparkConf() \
    .setMaster("local[*]") \
    .setAppName("Medallion-Bronze-Ingestion") \
    .set("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .set("spark.sql.catalog.iceberg_bronze", "org.apache.iceberg.spark.SparkCatalog") \
    .set("spark.sql.catalog.iceberg_bronze.type", "hadoop") \
    .set("spark.sql.catalog.iceberg_bronze.warehouse", iceberg_dir) \
    .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,io.delta:delta-spark_2.13:3.2.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.4.3")

builder = SparkSession.builder.config(conf=conf)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("🚀 [BRONZE] Ingestion Started.")


# Read Data and create tables
for filename in os.listdir(source_dir):
    if filename.endswith(".csv"):
        file_path = os.path.join(source_dir, filename)
        table_name = filename.replace(".csv", "").replace("AdventureWorks_", "").lower()

        print(f"📥 Reading: {filename}")
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)

        # 🪵 Format 1: Write Delta Lake table directly to Bronze folder
        delta_path = os.path.join(delta_dir,table_name)
        df.write.format("delta").mode("overwrite").save(delta_path)

        # 🧊 Format 2: Write Iceberg table using native SQL
        df.createOrReplaceTempView("Bronze_view")
        spark.sql(f"CREATE TABLE IF NOT EXISTS iceberg_bronze.`{table_name}` AS SELECT * FROM Bronze_view")
        spark.sql(f"INSERT OVERWRITE iceberg_bronze.`{table_name}` SELECT * FROM Bronze_view")

print("✅ All raw tables ingested side-by-side into the Bronze Directory.")

print("✅ Data successfully ingested into Bronze.")
spark.stop()
