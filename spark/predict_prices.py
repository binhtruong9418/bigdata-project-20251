"""
Zillow House Price Prediction - Predict ALL Data (Simple Version)
This script reads all data from Cassandra, trains a model, and makes predictions
No saving - just shows results
"""

import os
import sys

# Set HADOOP_HOME for Windows
if sys.platform == 'win32':
    hadoop_home = os.path.join(os.getcwd(), 'hadoop')
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] = os.path.join(hadoop_home, 'bin') + ';' + os.environ.get('PATH', '')

# Set Python executable for PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, avg, min, max, count

print("=" * 60)
print("Zillow Price Prediction - Predict All Data")
print("=" * 60)

# Initialize Spark with Cassandra
print("\n[1/6] Initializing Spark session...")
spark = SparkSession.builder \
    .appName("ZillowPredictAllData") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.ui.port", "4040") \
    .config("spark.local.dir", os.path.join(os.getcwd(), "spark-temp")) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("[OK] Spark session created")
print("[INFO] Spark Web UI available at: http://localhost:4040")

# Read ALL data from Cassandra
print("\n[2/6] Reading ALL data from Cassandra...")
try:
    df_all = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="data2", keyspace="finaldata1") \
        .load()

    total_rows = df_all.count()
    print(f"[OK] Loaded {total_rows:,} properties")

except Exception as e:
    print(f"[ERROR] Could not read from Cassandra: {e}")
    try:
        spark.stop()
    except:
        pass
    sys.exit(1)

# Preprocessing
print("\n[3/6] Preprocessing data...")

categorical_columns = ['city', 'hometype', 'newconstructiontype']
numeric_columns = ['lotareavalue', 'bathrooms', 'bedrooms', 'livingarea',
                  'isfeatured', 'isshowcaselisting', 'listingsubtype_is_newhome']

df_processed = df_all
for col_name in categorical_columns:
    df_processed = df_processed.fillna({col_name: 'UNKNOWN'})
for col_name in numeric_columns:
    df_processed = df_processed.fillna({col_name: 0})

df_processed = df_processed.filter((col("price").isNotNull()) & (col("price") > 0))

print(f"[OK] {df_processed.count():,} valid properties")

# Build Pipeline
print("\n[4/6] Building ML Pipeline...")

string_indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep") for c in categorical_columns]
one_hot_encoders = [OneHotEncoder(inputCol=f"{c}_index", outputCol=f"{c}_vec", handleInvalid="keep") for c in categorical_columns]
assembler = VectorAssembler(inputCols=[f"{c}_vec" for c in categorical_columns] + numeric_columns, outputCol="features", handleInvalid="keep")
rf = RandomForestRegressor(featuresCol="features", labelCol="price", numTrees=20, maxDepth=10, seed=42)

pipeline = Pipeline(stages=string_indexers + one_hot_encoders + [assembler, rf])

# Train
print("\n[5/6] Training model (this takes 2-3 minutes)...")
train_data, test_data = df_processed.randomSplit([0.8, 0.2], seed=42)

try:
    model = pipeline.fit(train_data)
    print("[OK] Training completed!")
except Exception as e:
    print(f"[ERROR] Training failed: {e}")
    try:
        spark.stop()
    except:
        pass
    sys.exit(1)

# Predict on ALL data
print("\n[6/6] Making predictions on all properties...")
predictions = model.transform(df_processed)
print("[OK] Predictions completed!")

# Show results
print("\n" + "=" * 60)
print("SAMPLE PREDICTIONS (First 20)")
print("=" * 60)

predictions.select(
    "zpid", "city", "hometype", "bedrooms", "bathrooms",
    "livingarea", "price", "prediction"
).withColumnRenamed("price", "actual_price") \
 .withColumnRenamed("prediction", "predicted_price") \
 .show(20, truncate=False)

# Statistics
print("\n" + "=" * 60)
print("OVERALL STATISTICS")
print("=" * 60)

stats = predictions.select(
    count("*").alias("total"),
    avg("price").alias("avg_actual"),
    avg("prediction").alias("avg_predicted"),
    min("prediction").alias("min_predicted"),
    max("prediction").alias("max_predicted")
).collect()[0]

print(f"\nTotal properties: {stats['total']:,}")
print(f"Avg actual price: ${stats['avg_actual']:,.2f}")
print(f"Avg predicted price: ${stats['avg_predicted']:,.2f}")
print(f"Min predicted: ${stats['min_predicted']:,.2f}")
print(f"Max predicted: ${stats['max_predicted']:,.2f}")

# By City
print("\n" + "=" * 60)
print("PREDICTIONS BY CITY")
print("=" * 60)

predictions.groupBy("city").agg(
    count("*").alias("count"),
    avg("price").alias("avg_actual"),
    avg("prediction").alias("avg_predicted")
).orderBy("avg_predicted", ascending=False).show(20, truncate=False)

# By Home Type
print("\n" + "=" * 60)
print("PREDICTIONS BY HOME TYPE")
print("=" * 60)

predictions.groupBy("hometype").agg(
    count("*").alias("count"),
    avg("price").alias("avg_actual"),
    avg("prediction").alias("avg_predicted")
).orderBy("avg_predicted", ascending=False).show(truncate=False)

print("\n" + "=" * 60)
print("COMPLETED!")
print("=" * 60)
print(f"\nSuccessfully predicted prices for {total_rows:,} properties!")
print("\nResults shown above include:")
print("  - Sample predictions")
print("  - Overall statistics")
print("  - Breakdown by city")
print("  - Breakdown by home type")
print("\n" + "=" * 60 + "\n")

# Keep session alive option
print("Spark Web UI: http://localhost:4040")
print("\nOptions:")
print("  1. Press ENTER to close Spark session and exit")
print("  2. Keep this window open to explore the Spark UI")
print("     (The UI will remain available while this script is running)")

try:
    input("\nPress ENTER when done exploring the UI...")
except KeyboardInterrupt:
    print("\n\nInterrupted by user")

# Clean shutdown
print("\nClosing Spark session...")
try:
    spark.stop()
except:
    pass

print("Done! All predictions completed successfully.")
