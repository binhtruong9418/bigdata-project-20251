"""
Batch Processing - Python 3.13 Compatible Version
Load data from HDFS to Cassandra using Pandas (no PySpark distributed operations)
"""

import json
import sys
from hdfs import InsecureClient

# Configure cassandra-driver for Python 3.13
import os
os.environ['CASSANDRA_DRIVER_NO_CYTHON'] = '1'  # Disable cython extensions
os.environ['CASSANDRA_DRIVER_NO_EXTENSIONS'] = '1'  # Disable all extensions

# Import gevent connection FIRST to set up event loop
from cassandra.io.geventreactor import GeventConnection

# IMPORTANT: Set as default connection before importing Cluster
import cassandra.io.geventreactor
cassandra.io.geventreactor.GeventConnection.initialize_reactor()

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
import pandas as pd

print("=" * 60)
print("Zillow Batch Processing - HDFS to Cassandra")
print("Python 3.13 Compatible Version using Pandas")
print("=" * 60)

# Get current date dynamically
year = datetime.now().strftime("%Y")
month = datetime.now().strftime("%m")
day = datetime.now().strftime("%d")

print(f"\nLooking for data in: /data/kafka_messages/{year}/{month}/{day}")

# Connect to HDFS
hdfs_client = InsecureClient("http://localhost:9870", user="root")

# Find all JSON files recursively
print("\n[1/5] Searching for JSON files in HDFS...")
all_files = []
try:
    for root, dirs, files in hdfs_client.walk('/data'):
        for filename in files:
            if filename.endswith('.json'):
                full_path = f"{root}/{filename}"
                all_files.append(full_path)

    if not all_files:
        print("\n[!] WARNING: No JSON files found in HDFS")
        print("\nRECOMMENDATION:")
        print("  1. Make sure consumer_batch.py is running")
        print("  2. Wait a few minutes for data to be collected")
        print("  3. Run this script again")
        sys.exit(1)

    print(f"[OK] Found {len(all_files)} JSON files")

except Exception as e:
    print(f"\n[ERROR] {str(e)}")
    print("\nRECOMMENDATION:")
    print("  1. Ensure consumer_batch.py is running")
    print("  2. Verify HDFS is accessible: http://localhost:9870")
    sys.exit(1)

# Read and combine all JSON files
print(f"\n[2/5] Reading {len(all_files)} JSON files from HDFS...")
all_data = []
files_read = 0

for file_path in all_files:
    try:
        with hdfs_client.read(file_path, encoding='utf-8') as reader:
            data = json.load(reader)
            if isinstance(data, list):
                all_data.extend(data)
            else:
                all_data.append(data)
        files_read += 1
        if files_read % 10 == 0:
            print(f"   Progress: {files_read}/{len(all_files)} files read...")
    except Exception as e:
        print(f"   Warning: Could not read {file_path}: {e}")

print(f"[OK] Read {files_read} files, total {len(all_data)} records")

if not all_data:
    print("\n[!] WARNING: No data found in JSON files")
    sys.exit(1)

# Convert to DataFrame
print("\n[3/5] Converting to DataFrame and processing...")
df = pd.DataFrame(all_data)

# Drop unnecessary columns (same as batch_processing.py)
columns_to_drop = [
    "timestamp", "homeStatus", "detailUrl", "address",
    "streetAddress", "zipcode", "latitude", "longitude",
    "currency", "zestimate", "rentZestimate", "taxAssessedValue",
    "lotAreaUnit", "daysOnZillow", "isPreforeclosureAuction",
    "timeOnZillow", "isNonOwnerOccupied", "isPremierBuilder",
    "isZillowOwned", "imgSrc", "hasImage", "brokerName",
    "listingSubType.is_FSBA", "priceChange", "datePriceChanged",
    "openHouse", "priceReduction", "unit",
    "listingSubType.is_openHouse", "videoCount", "country", "state"
]

# Drop columns that exist
existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
df = df.drop(columns=existing_columns_to_drop)

# Rename columns to match Cassandra schema (lowercase)
rename_map = {
    "listingSubType.is_newHome": "listingsubtype_is_newhome",
    "homeType": "hometype",
    "lotAreaValue": "lotareavalue",
    "livingArea": "livingarea",
    "isFeatured": "isfeatured",
    "isShowcaseListing": "isshowcaselisting",
    "newConstructionType": "newconstructiontype"
}

for old_name, new_name in rename_map.items():
    if old_name in df.columns:
        df = df.rename(columns={old_name: new_name})

print(f"[OK] Processed {len(df)} rows")
print(f"\nColumns: {list(df.columns)}")
print(f"\nSample data:")
print(df.head(3))

# Connect to Cassandra
print("\n[4/5] Connecting to Cassandra...")
try:
    cluster = Cluster(['localhost'], port=9042, connection_class=GeventConnection)
    session = cluster.connect()
    print("[OK] Connected to Cassandra")

    # Create keyspace if not exists
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS finaldata1
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    print("[OK] Keyspace 'finaldata1' ready")

    # Create table if not exists
    session.execute("""
        CREATE TABLE IF NOT EXISTS finaldata1.data2 (
            zpid text PRIMARY KEY,
            city text,
            hometype text,
            newconstructiontype text,
            lotareavalue double,
            bathrooms int,
            bedrooms int,
            livingarea int,
            isfeatured boolean,
            isshowcaselisting boolean,
            listingsubtype_is_newhome boolean,
            price bigint
        )
    """)
    print("[OK] Table 'data2' ready")

    session.set_keyspace('finaldata1')

except Exception as e:
    print(f"[ERROR] Could not connect to Cassandra: {e}")
    print("\nRECOMMENDATION:")
    print("  1. Ensure Docker Cassandra container is running:")
    print("     docker-compose ps | grep cassandra")
    print("  2. Restart Docker services if needed:")
    print("     docker-compose restart cassandra")
    sys.exit(1)

# Insert data into Cassandra
print(f"\n[5/5] Inserting {len(df)} records into Cassandra...")

insert_query = """
    INSERT INTO data2 (zpid, city, hometype, newconstructiontype, lotareavalue,
                      bathrooms, bedrooms, livingarea, isfeatured, isshowcaselisting,
                      listingsubtype_is_newhome, price)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

prepared = session.prepare(insert_query)
inserted = 0
errors = 0

for index, row in df.iterrows():
    try:
        session.execute(prepared, (
            str(row.get('zpid', '')),
            row.get('city'),
            row.get('hometype'),
            row.get('newconstructiontype'),
            float(row.get('lotareavalue', 0)) if pd.notna(row.get('lotareavalue')) else None,
            int(row.get('bathrooms', 0)) if pd.notna(row.get('bathrooms')) else None,
            int(row.get('bedrooms', 0)) if pd.notna(row.get('bedrooms')) else None,
            int(row.get('livingarea', 0)) if pd.notna(row.get('livingarea')) else None,
            bool(row.get('isfeatured', False)),
            bool(row.get('isshowcaselisting', False)),
            bool(row.get('listingsubtype_is_newhome', False)),
            int(row.get('price', 0)) if pd.notna(row.get('price')) else None
        ))
        inserted += 1
        if inserted % 50 == 0:
            print(f"   Progress: {inserted}/{len(df)} records inserted...")
    except Exception as e:
        errors += 1
        if errors <= 3:  # Show first 3 errors only
            print(f"   Warning: Could not insert row {index}: {e}")

print(f"[OK] Inserted {inserted} records (errors: {errors})")

# Close connection
cluster.shutdown()

print("\n" + "=" * 60)
print("BATCH PROCESSING COMPLETED")
print("=" * 60)
print(f"\nTotal files processed: {files_read}")
print(f"Total records inserted: {inserted}")
print(f"Data written to: Cassandra keyspace 'finaldata1', table 'data2'")
print("\nNext steps:")
print("  1. Check data with: python check_cassandra_data.py")
print("  2. Run ML analysis: python spark/sparkML.py")
print("=" * 60 + "\n")
