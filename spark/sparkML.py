"""
Zillow House Price Prediction - Machine Learning with Cassandra Data
Python 3.11 Version - Reads real data from Cassandra and trains Random Forest model
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
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

print("=" * 60)
print("Zillow House Price Prediction - Machine Learning")
print("=" * 60)
print("\nReading real data from Cassandra database...")
print("Training Random Forest model for price prediction\n")

# Initialize SparkSession with Cassandra connector
spark = SparkSession.builder \
    .appName("ZillowHousePricePrediction") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Read data from Cassandra
print("[1/6] Reading data from Cassandra (keyspace: finaldata1, table: data2)...")
try:
    df_read = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="data2", keyspace="finaldata1") \
        .load()

    row_count = df_read.count()
    print(f"[OK] Successfully loaded {row_count:,} rows from Cassandra\n")

    if row_count == 0:
        print("[ERROR] No data found in Cassandra!")
        print("\nRECOMMENDATION:")
        print("  1. Run: python spark/batch_processing.py")
        print("  2. Wait for data to be loaded into Cassandra")
        print("  3. Run this script again")
        spark.stop()
        sys.exit(1)

    if row_count < 100:
        print(f"[WARNING] Only {row_count} rows available")
        print("  Recommended minimum: 100-200 rows for meaningful ML training")
        print("  Continue anyway? The model may not be accurate.\n")

except Exception as e:
    print(f"[ERROR] Could not read from Cassandra: {e}")
    print("\nTROUBLESHOOTING:")
    print("  1. Ensure Cassandra is running:")
    print("     docker-compose ps | grep cassandra")
    print("  2. Ensure data has been loaded:")
    print("     python check_cassandra_data.py")
    print("  3. Ensure batch_processing.py completed successfully")
    spark.stop()
    sys.exit(1)

# Data preprocessing
print("[2/6] Preprocessing data and feature engineering...")

# Define columns
categorical_columns = ['city', 'hometype', 'newconstructiontype']
numeric_columns = ['lotareavalue', 'bathrooms', 'bedrooms', 'livingarea',
                  'isfeatured', 'isshowcaselisting', 'listingsubtype_is_newhome']

# Fill NULL values in categorical columns
df_processed = df_read
for col_name in categorical_columns:
    df_processed = df_processed.fillna({col_name: 'UNKNOWN'})

# Fill NULL values in numeric columns with 0
for col_name in numeric_columns:
    df_processed = df_processed.fillna({col_name: 0})

# Remove rows where price is null or zero
df_processed = df_processed.filter((col("price").isNotNull()) & (col("price") > 0))

print(f"[OK] After preprocessing: {df_processed.count():,} rows")
print(f"     Features: {len(categorical_columns)} categorical + {len(numeric_columns)} numeric")

# Show sample data
print("\nSample data:")
df_processed.select("city", "hometype", "bedrooms", "bathrooms", "livingarea", "price").show(5, truncate=False)

# Build ML Pipeline
print("\n[3/6] Building ML Pipeline...")

# Create string indexers for categorical features
string_indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
    for col in categorical_columns
]

# Create one-hot encoders
one_hot_encoders = [
    OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_vec", handleInvalid="keep")
    for col in categorical_columns
]

# Create vector assembler
assembler_inputs = [f"{col}_vec" for col in categorical_columns] + numeric_columns
assembler = VectorAssembler(
    inputCols=assembler_inputs,
    outputCol="features",
    handleInvalid="keep"
)

# Create Random Forest model
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="price",
    numTrees=20,  # Increased from 10 for better accuracy
    maxDepth=10,   # Increased depth
    seed=42
)

# Build pipeline
pipeline = Pipeline(stages=string_indexers + one_hot_encoders + [assembler, rf])

print("[OK] Pipeline created with stages:")
print("     - String Indexers (3)")
print("     - One-Hot Encoders (3)")
print("     - Vector Assembler")
print("     - Random Forest Regressor (20 trees, max depth 10)")

# Split data
print("\n[4/6] Splitting data into training and test sets (80/20)...")
train_data, test_data = df_processed.randomSplit([0.8, 0.2], seed=42)
train_count = train_data.count()
test_count = test_data.count()
print(f"[OK] Training set: {train_count:,} rows")
print(f"     Test set: {test_count:,} rows")

# Train model
print("\n[5/6] Training Random Forest model...")
print("     This may take a few minutes...")

try:
    model = pipeline.fit(train_data)
    print("[OK] Model training completed!")
except Exception as e:
    print(f"[ERROR] Model training failed: {e}")
    print("\nThis might be due to insufficient data or data quality issues.")
    spark.stop()
    sys.exit(1)

# Make predictions
print("\n[6/6] Evaluating model on test set...")
predictions = model.transform(test_data)

# Evaluate model
evaluator_rmse = RegressionEvaluator(
    labelCol="price",
    predictionCol="prediction",
    metricName="rmse"
)

evaluator_r2 = RegressionEvaluator(
    labelCol="price",
    predictionCol="prediction",
    metricName="r2"
)

evaluator_mae = RegressionEvaluator(
    labelCol="price",
    predictionCol="prediction",
    metricName="mae"
)

rmse = evaluator_rmse.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)
mae = evaluator_mae.evaluate(predictions)

print("\n" + "=" * 60)
print("MODEL EVALUATION RESULTS")
print("=" * 60)
print(f"\nRoot Mean Squared Error (RMSE): ${rmse:,.2f}")
print(f"R² Score: {r2:.4f}")
print(f"Mean Absolute Error (MAE): ${mae:,.2f}")

# Show sample predictions
print("\n" + "=" * 60)
print("SAMPLE PREDICTIONS")
print("=" * 60)
print("\nComparing actual vs predicted prices (first 10 test samples):\n")

predictions.select("city", "hometype", "bedrooms", "bathrooms", "livingarea",
                  "price", "prediction") \
    .withColumn("error", col("prediction") - col("price")) \
    .withColumn("error_pct", (col("error") / col("price")) * 100) \
    .show(10, truncate=False)

# Feature importance (from Random Forest model)
print("\n" + "=" * 60)
print("FEATURE IMPORTANCE")
print("=" * 60)

rf_model = model.stages[-1]  # Get the Random Forest model from pipeline
feature_importance = rf_model.featureImportances.toArray()
feature_names = assembler_inputs

# Sort features by importance
importance_list = list(zip(feature_names, feature_importance))
importance_list.sort(key=lambda x: x[1], reverse=True)

print("\nTop features influencing price prediction:\n")
for i, (feature, importance) in enumerate(importance_list[:10], 1):
    print(f"{i:2d}. {feature:30s}: {importance:.4f}")

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"\nTotal data points: {row_count:,}")
print(f"Training samples: {train_count:,}")
print(f"Test samples: {test_count:,}")
print(f"Model accuracy (R²): {r2:.2%}")
print(f"Average prediction error: ${mae:,.2f}")

if r2 > 0.7:
    print("\n✓ Model shows GOOD predictive performance!")
elif r2 > 0.5:
    print("\n~ Model shows MODERATE predictive performance")
else:
    print("\n! Model shows LOW predictive performance")
    print("  Consider collecting more data or adding more features")

print("\n" + "=" * 60)
print("ML TRAINING COMPLETED SUCCESSFULLY")
print("=" * 60)
print("\nThe model has been trained and evaluated.")
print("You can now use it to predict house prices based on property features.\n")

# Stop Spark session
spark.stop()
