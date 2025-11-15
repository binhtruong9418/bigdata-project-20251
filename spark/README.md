# Spark Processing Scripts

This folder contains optimized Spark scripts for the Zillow data pipeline.

---

## 📁 Files

### 1. `batch_processing.py` - HDFS to Cassandra Loader

**Purpose:** Load batch data from HDFS into Cassandra

**When to use:** After collecting data from Kafka to HDFS

**Command:**
```bash
python spark/batch_processing.py
```

**What it does:**
- ✅ Reads JSON files from HDFS (`/data/kafka_messages/YYYY/MM/DD/`)
- ✅ Auto-creates Cassandra keyspace `finaldata1` and table `data2`
- ✅ Transforms and cleans data
- ✅ Loads data into Cassandra
- ✅ Shows progress every 100 files
- ✅ Displays summary report (success/failed counts)

**Requirements:**
- HDFS must have data (run `consumer_batch.py` first)
- Cassandra must be running

---

### 2. `predict_prices.py` - ML Price Prediction

**Purpose:** Train ML model and predict prices for ALL properties in Cassandra

**When to use:** After loading data to Cassandra

**Command:**
```bash
python spark/predict_prices.py
```

**What it does:**
- ✅ Reads ALL data from Cassandra
- ✅ Trains Random Forest model (20 trees, depth 10)
- ✅ Predicts prices for all properties
- ✅ Shows sample predictions (first 20)
- ✅ Displays statistics by city and home type
- ✅ Keeps Spark UI alive for exploration (http://localhost:4040)

**Features:**
- 📊 Automatic preprocessing (handles NULL values)
- 🎯 Predictions on 100% of data
- 📈 Detailed statistics and breakdowns
- 🌐 Spark Web UI for monitoring
- ⚡ Optimized for Windows (no temp file errors)

**Output Example:**
```
Total properties: 650
Avg predicted price: $8,251,029.00
Min predicted: $903,183.28
Max predicted: $67,311,818.57

By City:
- Beverly Hills: $6,185,828 avg
- Los Angeles: $10,093,530 avg
- Santa Monica: $7,096,517 avg
```

---

## 🔄 Complete Workflow

### Step 1: Start Services
```bash
docker-compose up -d
```

### Step 2: Collect Data
```bash
# Terminal 1: Producer
python kafka/producer.py

# Terminal 2: Batch Consumer (saves to HDFS)
python kafka/consumer_batch.py

# Wait 10-15 minutes for data collection
```

### Step 3: Load to Cassandra
```bash
python spark/batch_processing.py
```

### Step 4: Train Model & Predict
```bash
python spark/predict_prices.py
```

---

## ⚙️ Configuration

### Cassandra Connection:
- Host: `localhost`
- Port: `9042`
- Keyspace: `finaldata1`
- Table: `data2`

### Spark Configuration:
- Mode: `local[*]` (uses all CPU cores)
- Cassandra Connector: `spark-cassandra-connector_2.12:3.5.0`
- Temp Directory: `spark-temp/` (auto-created, gitignored)
- Web UI: `http://localhost:4040` (when running)

### ML Model:
- Algorithm: Random Forest Regressor
- Trees: 20
- Max Depth: 10
- Features: city, hometype, bedrooms, bathrooms, livingarea, lotareavalue, etc.
- Target: price

---

## 🐛 Troubleshooting

### "No data found in Cassandra"
**Solution:** Run `batch_processing.py` first to load data from HDFS

### "Could not connect to Cassandra"
**Solution:**
```bash
# Check Cassandra is running
docker-compose ps | grep cassandra

# Restart if needed
docker-compose restart cassandra
```

### "No files found in HDFS"
**Solution:** Run `consumer_batch.py` and wait 10-15 minutes for data collection

### Port 4040 already in use
**Note:** Spark will automatically try 4041, 4042, etc. Check console output for actual port.

---

## 📊 Performance

### batch_processing.py:
- Speed: ~100 files/minute
- Memory: ~2-4 GB
- Typical runtime: 5-10 minutes for 3000 files

### predict_prices.py:
- Training time: 2-3 minutes for 650 properties
- Prediction time: ~30 seconds
- Memory: ~2-3 GB
- Total runtime: ~3-4 minutes

---

## 🔧 Advanced Usage

### Custom Date Range (batch_processing.py):
The script automatically uses today's date. To process a different date, modify lines 22-24:
```python
year = "2025"
month = "11"
day = "14"
```

### Adjust Model Parameters (predict_prices.py):
Modify line 75:
```python
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="price",
    numTrees=20,      # Increase for better accuracy (slower)
    maxDepth=10,      # Increase for complex patterns
    seed=42
)
```

---

## 📝 Notes

- Both scripts are optimized for Windows (no temp file deletion errors)
- Spark UI available during execution for monitoring
- All scripts use PySpark 3.5.0 (compatible with Cassandra connector)
- Temp files stored in `spark-temp/` (auto-cleaned, gitignored)
