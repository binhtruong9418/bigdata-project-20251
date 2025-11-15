"""
Check if Cassandra has enough data for ML training
Run this before executing sparkML.py
"""

import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def check_cassandra_data():
    """
    Connects to Cassandra and checks data availability in finaldata1.data2 table
    """
    print("=" * 60)
    print("Cassandra Data Checker for ML Training")
    print("=" * 60)
    print()

    try:
        # Connect to Cassandra
        print("[1/4] Connecting to Cassandra (localhost:9042)...")
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()
        print("✓ Connected successfully")
        print()

        # Check if keyspace exists
        print("[2/4] Checking keyspace 'finaldata1'...")
        keyspaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces").all()
        keyspace_names = [row.keyspace_name for row in keyspaces]

        if 'finaldata1' not in keyspace_names:
            print("✗ Keyspace 'finaldata1' does not exist!")
            print()
            print("RECOMMENDATION:")
            print("  Run batch_processing.py first to create keyspace and load data:")
            print("  python spark/batch_processing.py")
            return False

        print("✓ Keyspace 'finaldata1' exists")
        print()

        # Check if table exists
        print("[3/4] Checking table 'data2'...")
        session.set_keyspace('finaldata1')

        tables = session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'finaldata1'"
        ).all()
        table_names = [row.table_name for row in tables]

        if 'data2' not in table_names:
            print("✗ Table 'data2' does not exist in keyspace 'finaldata1'!")
            print()
            print("RECOMMENDATION:")
            print("  Run batch_processing.py first to create table and load data:")
            print("  python spark/batch_processing.py")
            return False

        print("✓ Table 'data2' exists")
        print()

        # Count rows
        print("[4/4] Counting rows in finaldata1.data2...")
        count_result = session.execute("SELECT COUNT(*) FROM finaldata1.data2").one()
        row_count = count_result.count

        print(f"✓ Found {row_count:,} rows")
        print()

        # Get sample statistics
        if row_count > 0:
            print("=" * 60)
            print("DATA STATISTICS")
            print("=" * 60)
            print()

            # Sample 5 rows
            print("Sample data (first 5 rows):")
            sample = session.execute("SELECT * FROM finaldata1.data2 LIMIT 5").all()

            for i, row in enumerate(sample, 1):
                print(f"\nRow {i}:")
                print(f"  City: {row.city}")
                print(f"  Price: ${row.price:,}")
                print(f"  Bedrooms: {row.bedrooms}")
                print(f"  Bathrooms: {row.bathrooms}")
                print(f"  Living Area: {row.livingarea} sqft")
                print(f"  Home Type: {row.hometype}")

            print()
            print("=" * 60)
            print("RECOMMENDATION FOR ML TRAINING")
            print("=" * 60)
            print()

            # Provide recommendation based on row count
            if row_count < 100:
                print(f"⚠ WARNING: Only {row_count} rows available")
                print("  Minimum recommended: 100-200 rows")
                print("  Status: NOT ENOUGH DATA")
                print()
                print("  ACTION: Wait for more data to be collected")
                print("  - Keep producer.py and consumer_batch.py running")
                print("  - Run batch_processing.py periodically to load HDFS data to Cassandra")
                print("  - Check again in 10-15 minutes")
                return False

            elif row_count < 500:
                print(f"⚠ CAUTION: {row_count} rows available")
                print("  Minimum recommended: 100-200 rows")
                print("  Optimal recommended: 1000+ rows")
                print("  Status: MINIMAL DATA (can train but results may vary)")
                print()
                print("  You can proceed with ML training, but:")
                print("  - Model accuracy may be limited")
                print("  - Consider waiting for more data for better results")
                print()
                print("  To proceed:")
                print("  python spark/sparkML.py")
                return True

            else:
                print(f"✓ GOOD: {row_count} rows available")
                print("  Optimal recommended: 1000+ rows")
                print("  Status: READY FOR ML TRAINING")
                print()
                print("  You have sufficient data for ML training.")
                print()
                print("  To proceed:")
                print("  python spark/sparkML.py")
                print()
                print("  Note: If using Python 3.13, sparkML.py has compatibility issues.")
                print("        Use Python 3.8-3.11 for full ML functionality.")
                return True
        else:
            print("=" * 60)
            print("⚠ NO DATA FOUND")
            print("=" * 60)
            print()
            print("RECOMMENDATION:")
            print("  1. Ensure producer.py is running to generate data")
            print("  2. Ensure consumer_batch.py is running to save to HDFS")
            print("  3. Run batch_processing.py to load data from HDFS to Cassandra:")
            print("     python spark/batch_processing.py")
            print("  4. Run this check again after 10-15 minutes")
            return False

    except Exception as e:
        print(f"✗ Error: {e}")
        print()
        print("TROUBLESHOOTING:")
        print("  1. Ensure Cassandra is running in Docker:")
        print("     docker-compose ps | grep cassandra")
        print("  2. Ensure Docker services are up:")
        print("     docker-compose up -d")
        print("  3. Install cassandra-driver if missing:")
        print("     pip install cassandra-driver")
        return False

    finally:
        if 'cluster' in locals():
            cluster.shutdown()
            print()
            print("Connection closed.")

if __name__ == "__main__":
    print()
    result = check_cassandra_data()
    print()
    sys.exit(0 if result else 1)
