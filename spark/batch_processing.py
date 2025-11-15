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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, LongType
from datetime import datetime
from cassandra.cluster import Cluster

def setup_cassandra():
    """Setup Cassandra keyspace and table if they don't exist"""
    print("\n[Setup] Configuring Cassandra database...")
    try:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()

        # Create keyspace
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS finaldata1
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)

        # Use keyspace
        session.set_keyspace('finaldata1')

        # Create table
        session.execute("""
            CREATE TABLE IF NOT EXISTS data2 (
                zpid text PRIMARY KEY,
                city text,
                hometype text,
                price bigint,
                lotareavalue double,
                bathrooms int,
                bedrooms int,
                livingarea int,
                isfeatured boolean,
                isshowcaselisting boolean,
                newconstructiontype text,
                listingsubtype_is_newhome boolean
            )
        """)

        cluster.shutdown()
        print("[OK] Cassandra keyspace 'finaldata1' and table 'data2' ready")
        return True
    except Exception as e:
        print(f"[ERROR] Cassandra setup failed: {e}")
        print("\nPlease ensure Cassandra is running on localhost:9042")
        return False

def get_hdfs_files(hdfs_client, year, month, day):
    """Get list of JSON files from HDFS for the specified date"""
    hdfs_path_forward = f"/data/kafka_messages/{year}/{month}/{day}"

    # Try forward slash path first (correct format)
    try:
        file_name_list = hdfs_client.list(hdfs_path_forward)
        if file_name_list:
            return [f"{hdfs_path_forward}/{file_name}" for file_name in file_name_list]
    except:
        pass

    # If not found, try recursive search
    try:
        all_files = []
        for root, dirs, files in hdfs_client.walk('/data'):
            for filename in files:
                if filename.endswith('.json'):
                    all_files.append(f"{root}/{filename}")

        if all_files:
            print(f"[OK] Found {len(all_files)} JSON files in /data (recursive search)")
            return all_files
    except Exception as e:
        print(f"\n[ERROR] Could not find data: {str(e)}")

    return []

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Zillow Data Processing") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

def get_json_schema():
    """Define the JSON schema for Zillow data"""
    return StructType([
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

def transform_dataframe(df):
    """Transform DataFrame by dropping unnecessary columns and renaming"""
    # Drop unnecessary columns
    df_clean = df.drop(
        "timestamp", "homeStatus", "detailUrl", "address", "streetAddress",
        "zipcode", "latitude", "longitude", "currency", "zestimate",
        "rentZestimate", "taxAssessedValue", "lotAreaUnit", "daysOnZillow",
        "isPreforeclosureAuction", "timeOnZillow", "isNonOwnerOccupied",
        "isPremierBuilder", "isZillowOwned", "imgSrc", "hasImage",
        "brokerName", "listingSubType.is_FSBA", "priceChange",
        "datePriceChanged", "openHouse", "priceReduction", "unit",
        "listingSubType.is_openHouse", "videoCount", "country", "state"
    )

    # Rename columns to match Cassandra table
    df_clean = df_clean \
        .withColumnRenamed("listingSubType.is_newHome", "listingsubtype_is_newhome") \
        .withColumnRenamed("homeType", "hometype") \
        .withColumnRenamed("lotAreaValue", "lotareavalue") \
        .withColumnRenamed("livingArea", "livingarea") \
        .withColumnRenamed("isFeatured", "isfeatured") \
        .withColumnRenamed("isShowcaseListing", "isshowcaselisting") \
        .withColumnRenamed("newConstructionType", "newconstructiontype")

    return df_clean

def main():
    """Main execution function"""
    # Get current date
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")

    print("=" * 60)
    print("Zillow Batch Processing - HDFS to Cassandra")
    print("=" * 60)
    print(f"\nLooking for data in: /data/kafka_messages/{year}/{month}/{day}")

    # Setup Cassandra
    if not setup_cassandra():
        sys.exit(1)

    # Connect to HDFS
    print("\n[1/4] Connecting to HDFS...")
    try:
        hdfs_client = InsecureClient("http://localhost:9870", user="root")
        print("[OK] Connected to HDFS")
    except Exception as e:
        print(f"[ERROR] Failed to connect to HDFS: {e}")
        print("\nVerify HDFS is accessible: http://localhost:9870")
        sys.exit(1)

    # Get file list from HDFS
    print("\n[2/4] Searching for data files...")
    hdfs_path_list = get_hdfs_files(hdfs_client, year, month, day)

    if not hdfs_path_list:
        print("\n[!] WARNING: No batch files found")
        print("\nRECOMMENDATION:")
        print("  1. Ensure consumer_batch.py is running")
        print("  2. Wait a few minutes for data to be collected")
        print("  3. Run this script again")
        sys.exit(1)

    print(f"[OK] Found {len(hdfs_path_list)} files to process")

    # Create Spark session
    print("\n[3/4] Initializing Spark session...")
    spark = create_spark_session()
    print("[OK] Spark session created")

    # Process files
    print("\n[4/4] Processing and loading data to Cassandra...")
    json_schema = get_json_schema()
    processed_count = 0

    for idx, hdfs_path in enumerate(hdfs_path_list, 1):
        try:
            # Read JSON from HDFS
            with hdfs_client.read(hdfs_path) as reader:
                data = json.loads(reader.read())

            # Create DataFrame and transform
            df = spark.createDataFrame(data, schema=json_schema)
            df_clean = transform_dataframe(df)

            # Show sample for first file
            if idx == 1:
                print("\nSample data (first file):")
                df_clean.show(10, truncate=False)

            # Write to Cassandra
            df_clean.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="data2", keyspace="finaldata1") \
                .save()

            processed_count += 1

            # Progress indicator
            if idx % 100 == 0:
                print(f"[Progress] Processed {idx}/{len(hdfs_path_list)} files...")

        except Exception as e:
            print(f"[WARNING] Failed to process {hdfs_path}: {e}")
            continue

    # Summary
    print("\n" + "=" * 60)
    print("BATCH PROCESSING COMPLETED")
    print("=" * 60)
    print(f"\nTotal files found: {len(hdfs_path_list)}")
    print(f"Successfully processed: {processed_count}")
    print(f"Failed: {len(hdfs_path_list) - processed_count}")
    print(f"\nData written to: Cassandra keyspace 'finaldata1', table 'data2'")
    print("\nNext steps:")
    print("  1. Verify data: SELECT COUNT(*) FROM finaldata1.data2;")
    print("  2. Run ML analysis: python spark/sparkML.py")
    print("=" * 60 + "\n")

    spark.stop()

if __name__ == "__main__":
    main()
