from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ZillowHousePricePrediction") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Read data from Cassandra
df_read = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="data2", keyspace="finaldata1") \
    .load()

# Define columns
categorical_columns = ['city', 'hometype', 'newconstructiontype']
numeric_columns = ['lotareavalue', 'bathrooms', 'bedrooms', 'livingarea', 
                  'isfeatured', 'isshowcaselisting', 'listingsubtype_is_newhome']

# Check for NULL values
print("Checking for NULL values in categorical columns:")
for col in categorical_columns:
    null_count = df_read.filter(df_read[col].isNull()).count()
    print(f"{col}: {null_count} NULL values")

# Fill NULL values
df_processed = df_read
for col in categorical_columns:
    df_processed = df_processed.fillna({col: 'UNKNOWN'})

# Create string indexers
string_indexers = [StringIndexer(inputCol=col, 
                               outputCol=f"{col}_index",
                               handleInvalid="keep")
                  for col in categorical_columns]

# Create one-hot encoders
one_hot_encoders = [OneHotEncoder(inputCol=f"{col}_index", 
                                outputCol=f"{col}_vec",
                                handleInvalid="keep")
                   for col in categorical_columns]

# Create vector assembler
assembler_inputs = [f"{col}_vec" for col in categorical_columns] + numeric_columns
assembler = VectorAssembler(inputCols=assembler_inputs, 
                          outputCol="features",
                          handleInvalid="keep")

# Create Random Forest model
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="price",
    numTrees=10,
    maxDepth=5,
    seed=42
)

# Create pipeline
pipeline = Pipeline(stages=string_indexers + one_hot_encoders + [assembler, rf])

# Split data
(train_data, test_data) = df_processed.randomSplit([0.7, 0.3], seed=42)

# Train model
print("Training Random Forest model...")
model = pipeline.fit(train_data)

# Make predictions
predictions = model.transform(test_data)

# Evaluate model
evaluator = RegressionEvaluator(
    labelCol="price",
    predictionCol="prediction",
    metricName="rmse"
)

rmse = evaluator.evaluate(predictions)
r2 = evaluator.setMetricName("r2").evaluate(predictions)

print("\nModel Performance:")
print(f"Root Mean Square Error (RMSE): ${rmse:,.2f}")
print(f"R2 Score: {r2:.3f}")

# Show sample predictions
print("\nSample Predictions:")
predictions.select("price", "prediction", *numeric_columns).show(5)

# Stop Spark session
spark.stop()