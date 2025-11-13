from hdfs import InsecureClient
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, TimestampType, DoubleType, LongType
from datetime import datetime

day = datetime.now().strftime("%d")

hdfs_client = InsecureClient("http://localhost:9870", user="root")
file_name_list = hdfs_client.list(f"/data/kafka_messages/2024/12/{day}")
hdfs_path_list = [f"/data/kafka_messages/2024/12/{day}/{file_name}" for file_name in file_name_list]

spark = SparkSession.builder \
    .appName("Zillow Data Processing") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
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
    print(f"Data saved to Cassandra {hdfs_path}")

spark.stop()