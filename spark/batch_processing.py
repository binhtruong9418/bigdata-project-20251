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

from hdfs import InsecureClient
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, TimestampType, DoubleType, LongType
from datetime import datetime

# Get current date dynamically
year = datetime.now().strftime("%Y")
month = datetime.now().strftime("%m")
day = datetime.now().strftime("%d")

print("=" * 60)
print("Zillow Batch Processing - HDFS to Cassandra")
print("=" * 60)
print(f"\nLooking for data in: /data/kafka_messages/{year}/{month}/{day}")

hdfs_client = InsecureClient("http://localhost:9870", user="root")

# Check if directory exists (try both forward and backslash paths)
hdfs_path_forward = f"/data/kafka_messages/{year}/{month}/{day}"
hdfs_path_backslash = f"/data/kafka_messages\\{year}\\{month}\\{day}"

file_name_list = []
hdfs_path = None

# Try forward slash path first (correct format)
try:
    file_name_list = hdfs_client.list(hdfs_path_forward)
    hdfs_path = hdfs_path_forward
    print(f"[OK] Found data in forward-slash path: {hdfs_path}")
except:
    pass

# If not found, try backslash path (old Windows format)
if not file_name_list:
    try:
        # HDFS actually stores backslash paths as literal directory names
        # We need to check what's actually in /data/
        data_contents = hdfs_client.list("/data")
        print(f"\n[INFO] Contents of /data/: {data_contents}")

        # The backslash path is stored as a single directory name like "kafka_messages\2025\11\15"
        # We need to traverse this structure
        if len(data_contents) > 0:
            # Get first item (should be kafka_messages directory or file)
            first_item = data_contents[0]
            print(f"[INFO] First item in /data/: {first_item}")

            # List recursively to find all JSON files
            import fnmatch
            all_files = []
            for root, dirs, files in hdfs_client.walk('/data'):
                for filename in files:
                    if filename.endswith('.json'):
                        full_path = f"{root}/{filename}"
                        all_files.append(full_path)

            if all_files:
                file_name_list = all_files
                hdfs_path = "/data"
                print(f"[OK] Found {len(all_files)} JSON files in /data (recursive search)")
                hdfs_path_list = all_files
    except Exception as e2:
        print(f"\n[ERROR] Could not find data: {str(e2)}")
        print("\nRECOMMENDATION:")
        print("  1. Ensure consumer_batch.py is running:")
        print("     python kafka/consumer_batch.py")
        print("  2. Wait 5-10 minutes for data to be collected to HDFS")
        print("  3. Verify HDFS is accessible: http://localhost:9870")
        print("  4. Run this script again")
        sys.exit(1)

# Check if we found any files
if not file_name_list:
    print(f"\n[!] WARNING: No batch files found")
    print("\nRECOMMENDATION:")
    print("  1. Make sure consumer_batch.py is running")
    print("  2. Wait a few minutes for data to be collected")
    print("  3. Run this script again")
    sys.exit(1)

# Build path list if not already done
if 'hdfs_path_list' not in locals():
    hdfs_path_list = [f"{hdfs_path}/{file_name}" for file_name in file_name_list]

print(f"[OK] Ready to process {len(hdfs_path_list)} files")

spark = SparkSession.builder \
    .appName("Zillow Data Processing") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()
json_schema = StructType([
        StructField("timestamp", LongType(), True),
        StructField("zpid", StringType(), True),
        StructField("homeStatus", StringType(), True),
        StructField("detailUrl", StringType(), True),
        StructField("address", StringType(), True),
        StructField("streetAddress", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("homeType", StringType(), True),
        StructField("price", LongType(), True),
        StructField("currency", StringType(), True),
        StructField("zestimate", IntegerType(), True),
        StructField("rentZestimate", IntegerType(), True),
        StructField("taxAssessedValue", IntegerType(), True),
        StructField("lotAreaValue", DoubleType(), True),
        StructField("lotAreaUnit", StringType(), True),
        StructField("bathrooms", IntegerType(), True),
        StructField("bedrooms", IntegerType(), True),
        StructField("livingArea", IntegerType(), True),
        StructField("daysOnZillow", IntegerType(), True),
        StructField("isFeatured", BooleanType(), True),
        StructField("isPreforeclosureAuction", BooleanType(), True),
        StructField("timeOnZillow", IntegerType(), True),
        StructField("isNonOwnerOccupied", BooleanType(), True),
        StructField("isPremierBuilder", BooleanType(), True),
        StructField("isZillowOwned", BooleanType(), True),
        StructField("isShowcaseListing", BooleanType(), True),
        StructField("imgSrc", StringType(), True),
        StructField("hasImage", BooleanType(), True),
        StructField("brokerName", StringType(), True),
        StructField("listingSubType.is_FSBA", BooleanType(), True),
        StructField("priceChange", IntegerType(), True),
        StructField("datePriceChanged", LongType(), True),
        StructField("openHouse", StringType(), True),
        StructField("priceReduction", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("listingSubType.is_openHouse", BooleanType(), True),
        StructField("newConstructionType", StringType(), True),
        StructField("listingSubType.is_newHome", BooleanType(), True),
        StructField("videoCount", IntegerType(), True)
    ])
for hdfs_path in hdfs_path_list:
    with hdfs_client.read(hdfs_path) as reader:
        data = reader.read()

    data = json.loads(data)

    df = spark.createDataFrame(data, schema=json_schema)
    df_drop = df.drop("timestamp","homeStatus","detailUrl","address", 
                      "streetAddress","zipcode","latitude","longitude",
                      "currency","zestimate","rentZestimate","taxAssessedValue",
                      "lotAreaUnit",
                      "daysOnZillow","isPreforeclosureAuction","timeOnZillow",
                      "isNonOwnerOccupied","isPremierBuilder","isZillowOwned",
                      "imgSrc","hasImage","brokerName","listingSubType.is_FSBA","priceChange",
                      "datePriceChanged","openHouse","priceReduction","unit","listingSubType.is_openHouse",
                      "videoCount", "country", "state")
    df_drop = df_drop.withColumnRenamed("listingSubType.is_newHome", "listingsubtype_is_newhome")
    df_drop = df_drop.withColumnRenamed("homeType", "hometype")
    df_drop = df_drop.withColumnRenamed("lotAreaValue", "lotareavalue")
    df_drop = df_drop.withColumnRenamed("livingArea", "livingarea")
    df_drop = df_drop.withColumnRenamed("isFeatured", "isfeatured")
    df_drop = df_drop.withColumnRenamed("isShowcaseListing", "isshowcaselisting")
    df_drop = df_drop.withColumnRenamed("newConstructionType", "newconstructiontype")
    df_drop.show()
    #write to cassandra
    df_drop.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="data2", keyspace="finaldata1") \
        .save()
    print(f"[OK] Data saved to Cassandra from {hdfs_path}")

print("\n" + "=" * 60)
print("BATCH PROCESSING COMPLETED")
print("=" * 60)
print(f"\nTotal files processed: {len(hdfs_path_list)}")
print(f"Data written to: Cassandra keyspace 'finaldata1', table 'data2'")
print("\nNext steps:")
print("  1. Check data with: python check_cassandra_data.py")
print("  2. Run ML analysis: python spark/sparkML.py")
print("=" * 60 + "\n")

spark.stop()