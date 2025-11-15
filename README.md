# Zillow Real Estate Data Pipeline - Nhóm 14

[Báo cáo BTL BigData - Nhóm 14 - Lớp 154050](https://docs.google.com/document/d/1Svi3nbpFZvkNQm9AJzbJgJ29YFHlan6-cYaCBs4nb8U/edit?tab=t.0)

## 📋 Mô tả dự án

Hệ thống xử lý dữ liệu bất động sản real-time sử dụng công nghệ Big Data. Pipeline bao gồm:

1. **Data Ingestion**: Tạo dữ liệu giả lập nhà đất California và gửi vào Kafka
2. **Stream Processing**: Xử lý real-time với Spark Structured Streaming
3. **Batch Processing**: Lưu trữ batch data vào HDFS và Cassandra
4. **Machine Learning**: Train model và dự đoán giá nhà với Random Forest
5. **Analytics**: Aggregation theo thành phố, home type, và thống kê chi tiết

---

## 🏗️ Kiến trúc hệ thống

```
Producer (Python)
    ↓
Kafka Cluster (3 brokers)
    ↓
    ├── Spark Structured Streaming → Console Output (Real-time Analytics)
    │
    └── Batch Consumer → HDFS
            ↓
        Batch Processing (PySpark)
            ↓
        Cassandra → ML Prediction (predict_prices.py)
```

### Các thành phần:

- **Kafka Producer**: Sinh dữ liệu giả lập nhà đất (price, bedrooms, city, ...)
- **Kafka Cluster**: 3 brokers (ports 9092, 9093, 9094) + ZooKeeper
- **Spark Streaming**: Xử lý real-time, tính toán metrics theo time window
- **Batch Consumer**: Đọc từ Kafka, lưu batch vào HDFS
- **HDFS**: Lưu trữ phân tán dữ liệu batch
- **Batch Processing**: Load data từ HDFS vào Cassandra (tự động setup database)
- **Cassandra**: NoSQL database cho ML pipeline
- **ML Prediction**: Train model và dự đoán giá cho tất cả properties (Random Forest)

---

## 💻 Yêu cầu hệ thống

### Phần mềm cần thiết:
- **Python 3.11** (KHUYẾN NGHỊ - Full compatibility)
- **Java** >= 8 (JDK)
- **Docker** và **Docker Compose**
- **Git** (để clone project)

**Tại sao Python 3.11?**
- ✅ Hỗ trợ đầy đủ tất cả thư viện (Kafka, PySpark, Cassandra)
- ✅ cassandra-driver hoạt động hoàn hảo
- ✅ PySpark 3.5.0 stable với Cassandra connector
- ✅ Tương thích với toàn bộ Big Data stack

**Lưu ý**:
- Python 3.13 chưa được cassandra-driver hỗ trợ
- PySpark 4.0+ chưa tương thích với Cassandra Spark Connector
- Sử dụng Python 3.11 + PySpark 3.5.0 để tránh vấn đề tương thích

### Hệ điều hành:
- Windows 10/11, Linux, hoặc MacOS

---

## 🚀 Hướng dẫn cài đặt

### Bước 1: Clone project

```bash
git clone <repository-url>
cd bigdata-project-20251
```

### Bước 2: Tạo môi trường ảo và cài đặt dependencies

#### Windows:
```bash
python -m venv .venv
.venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

#### Linux/Mac:
```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

**Lưu ý quan trọng**: Nếu gặp lỗi import khi chạy PySpark, xóa `.venv` và tạo lại từ đầu:
```bash
# Windows
rmdir /s /q .venv
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Linux/Mac
rm -rf .venv
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Bước 3: Setup Hadoop cho Windows (chỉ Windows)

Đã được tự động setup trong code. Hadoop binaries sẽ được tải tự động vào folder `hadoop/bin/`.

### Bước 4: Khởi động Docker services

```bash
docker-compose up -d
```

Chờ khoảng 30-60 giây để các services khởi động hoàn toàn.

### Bước 5: Kiểm tra các services

```bash
docker-compose ps
```

Đảm bảo tất cả containers đang chạy (status: Up).

---

## 🎯 Chạy dự án

### Workflow hoàn chỉnh:

```
1. Start Docker services
2. Run Producer (tạo data)
3. Run Batch Consumer (lưu vào HDFS)
4. Run Streaming Consumer (real-time analytics)
5. Wait 10-15 minutes (collect data)
6. Run Batch Processing (HDFS → Cassandra, auto setup database)
7. Run ML Prediction (train model và predict ALL data từ Cassandra)
```

### Cách 1: Chạy tự động (Khuyến nghị)

#### Windows:
```bash
.\start.bat
```

#### Linux/Mac:
```bash
chmod +x start.sh
./start.sh
```

Script sẽ tự động chạy:
- Kafka Producer
- Batch Consumer (HDFS)
- Spark Streaming Consumer

### Cách 2: Chạy từng thành phần riêng lẻ

#### Terminal 1 - Kafka Producer:
```bash
python kafka/producer.py
```

#### Terminal 2 - Batch Consumer (HDFS):
```bash
python kafka/consumer_batch.py
```

#### Terminal 3 - Streaming Consumer (Spark):
```bash
python kafka/consumer_structured_stream.py
```

---

## 🤖 Machine Learning Workflow

### Bước 1: Collect data (đợi 10-15 phút)
```bash
# Producer, Batch Consumer, và Spark Streaming đang chạy
# Đợi để data được collect vào HDFS
# Kiểm tra: http://localhost:9870 → Utilities → Browse the file system → /data/kafka_messages
```

### Bước 2: Load data từ HDFS vào Cassandra
```bash
python spark/batch_processing.py
```

**Tính năng:**
- ✅ Tự động tạo Cassandra keyspace `finaldata1`
- ✅ Tự động tạo table `data2` với schema phù hợp
- ✅ Không cần chạy setup riêng
- ✅ Hiển thị progress bar khi xử lý nhiều files
- ✅ Error handling tốt, retry logic
- ✅ Summary report chi tiết (success/failed counts)

**Output mẫu:**
```
============================================================
Zillow Batch Processing - HDFS to Cassandra
============================================================

[Setup] Configuring Cassandra database...
[OK] Cassandra keyspace 'finaldata1' and table 'data2' ready

[1/4] Connecting to HDFS...
[OK] Connected to HDFS

[2/4] Searching for data files...
[OK] Found 2984 files to process

[3/4] Initializing Spark session...
[OK] Spark session created

[4/4] Processing and loading data to Cassandra...
[Progress] Processed 100/2984 files...
[Progress] Processed 200/2984 files...

============================================================
BATCH PROCESSING COMPLETED
============================================================

Total files found: 2984
Successfully processed: 2980
Failed: 4

Data written to: Cassandra keyspace 'finaldata1', table 'data2'
```

### Bước 3: Train Model & Predict ALL Data
```bash
python spark/predict_prices.py
```

**Tính năng mới:**
- ✅ Reads ALL data from Cassandra automatically
- ✅ Trains Random Forest model (20 trees, depth 10)
- ✅ Predicts prices for ALL properties
- ✅ Shows sample predictions (first 20)
- ✅ Statistics by city and home type
- ✅ **Spark Web UI available at http://localhost:4040**
- ✅ Keeps session alive - press ENTER when done exploring UI
- ✅ No temp file cleanup errors on Windows

**Output mẫu:**
```
============================================================
Zillow Price Prediction - Predict All Data
============================================================

[1/6] Initializing Spark session...
[OK] Spark session created
[INFO] Spark Web UI available at: http://localhost:4040

[2/6] Reading ALL data from Cassandra...
[OK] Loaded 650 properties

[3/6] Preprocessing data...
[OK] 650 valid properties

[4/6] Building ML Pipeline...

[5/6] Training model (this takes 2-3 minutes)...
[OK] Training completed!

[6/6] Making predictions on all properties...
[OK] Predictions completed!

============================================================
SAMPLE PREDICTIONS (First 20)
============================================================
+---------+-------------+-------------+------------+--------------------+
|zpid     |city         |hometype     |actual_price|predicted_price     |
+---------+-------------+-------------+------------+--------------------+
|255566386|Burbank      |MANUFACTURED |1787176     |3199940.03          |
|376355892|Beverly Hills|MULTI_FAMILY |8260909     |11710404.13         |
+---------+-------------+-------------+------------+--------------------+

============================================================
OVERALL STATISTICS
============================================================

Total properties: 650
Avg actual price: $7,980,586.12
Avg predicted price: $8,251,029.00
Min predicted: $903,183.28
Max predicted: $67,311,818.57

============================================================
PREDICTIONS BY CITY
============================================================
+--------------+-----+--------------------+
|city          |count|avg_predicted       |
+--------------+-----+--------------------+
|Sherman Oaks  |72   |11889660.33         |
|Beverly Hills |51   |6185828.02          |
+--------------+-----+--------------------+

Spark Web UI: http://localhost:4040

Press ENTER when done exploring the UI...
```

**Model features:**
- Algorithm: Random Forest Regressor
- Trees: 20
- Max Depth: 10
- Features: city, hometype, bedrooms, bathrooms, livingarea, lotareavalue, etc.
- Target: price prediction

---

## 📊 Kết quả mong đợi

### Producer Output:
```
Sent data: {'timestamp': 1763174687104, 'zpid': 336424216, 'city': 'Los Angeles', 'price': 7578356, ...}
Sent data: {'timestamp': 1763174687952, 'zpid': 261109533, 'city': 'Glendale', 'price': 7632629, ...}
```

### Batch Consumer Output:
```
2025-11-15 10:06:05,815 - __main__ - INFO - Batch of 10 messages saved to /data/kafka_messages/2025/11/15/10_06_05_843515_batch.json
```

### Spark Streaming Output:
```
+----------------------------------------------+--------------+-------------+-----------------+
|window                                        |city          |average_price|total_bedrooms   |
+----------------------------------------------+--------------+-------------+-----------------+
|{2025-11-15 10:00:00, 2025-11-15 10:01:00}   |Los Angeles   |6022806.0    |6                |
|{2025-11-15 10:00:00, 2025-11-15 10:01:00}   |Beverly Hills |8109473.0    |11               |
+----------------------------------------------+--------------+-------------+-----------------+
```

---

## 📁 Cấu trúc thư mục

```
bigdata-project-20251/
├── kafka/
│   ├── producer.py                  # Kafka producer
│   ├── consumer_batch.py            # Batch consumer → HDFS
│   └── consumer_structured_stream.py # Spark streaming consumer
├── spark/
│   ├── batch_processing.py          # HDFS → Cassandra (optimized, auto setup)
│   ├── predict_prices.py            # ML prediction for ALL data (with Spark UI)
│   └── README.md                    # Spark scripts documentation
├── hadoop/
│   └── bin/                         # Hadoop binaries (Windows only)
├── check_cassandra_data.py          # Verify Cassandra data
├── docker-compose.yml               # Docker services configuration
├── requirements.txt                 # Python dependencies (PySpark 3.5.0)
├── .gitignore                       # Git ignore rules
├── start.bat                        # Windows startup script
├── start.sh                         # Linux/Mac startup script
├── stop.bat                         # Windows stop script
├── stop.sh                          # Linux/Mac stop script
├── CLEANUP.md                       # Complete cleanup guide
└── README.md                        # This file
```

---

## 🌐 Web UIs

Sau khi khởi động Docker services, có thể truy cập:

- **Spark Master**: http://localhost:8080
- **Spark Worker**: http://localhost:8081
- **HDFS NameNode**: http://localhost:9870
  - Browse files: Utilities → Browse the file system → /data/kafka_messages
- **Spark Application UI**: http://localhost:4040 (khi chạy predict_prices.py)

---

## 📦 Dependencies chính

```
# Core Big Data Processing
pyspark==3.5.0                      # Tương thích với Cassandra connector
py4j==0.10.9.7

# Kafka
kafka-python-ng                      # Kafka client (Python 3.11+ compatible)

# Data Processing
pandas
numpy

# Storage Connectors
hdfs                                 # HDFS client
cassandra-driver                     # Cassandra client (Python 3.11 compatible)
```

**Version Compatibility Matrix**:
| Component | Version | Reason |
|-----------|---------|--------|
| PySpark | 3.5.0 | Compatible with Cassandra connector 3.5.0 |
| Cassandra Connector | 3.5.0 (Scala 2.12) | Matches Spark 3.5.0 Scala version |
| Python | 3.11 | Full cassandra-driver support |

---

## 🐛 Troubleshooting

### 1. Import Error: `cannot import name 'is_remote_only'`
**Nguyên nhân**: Virtual environment bị corrupt với mixed PySpark versions

**Giải pháp**:
```bash
# Windows
deactivate
rmdir /s /q .venv
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Linux/Mac
deactivate
rm -rf .venv
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Cassandra Connection Error
**Nguyên nhân**: Cassandra chưa sẵn sàng

**Giải pháp**:
```bash
# Kiểm tra Cassandra status
docker-compose ps

# Restart Cassandra
docker-compose restart cassandra

# Đợi 30-60 giây rồi thử lại
```

### 3. Spark Cassandra Connector Error: `NoClassDefFoundError: scala/$less$colon$less`
**Nguyên nhân**: Sai Scala version trong connector

**Giải pháp**: Đã fix trong code, sử dụng `_2.12` thay vì `_2.13`

### 4. Docker containers không start
```bash
docker-compose down
docker-compose up -d
```

### 5. Port đã được sử dụng
```bash
# Windows
netstat -ano | findstr :9092
taskkill /PID <PID> /F

# Linux/Mac
lsof -i :9092
kill -9 <PID>
```

### 6. HDFS không accessible
```bash
# Kiểm tra HDFS NameNode
docker logs namenode

# Restart HDFS
docker-compose restart namenode datanode
```

### 7. Spark temp file cleanup errors (Windows)
**Nguyên nhân**: Windows file locking

**Giải pháp**: Đã fix - script sử dụng local `spark-temp/` directory và graceful shutdown. Errors (nếu có) là cosmetic và không ảnh hưởng kết quả.

---

## 🔧 Optimizations

### Batch Processing Script (`batch_processing.py`):
1. **Integrated Cassandra Setup**: Tự động tạo keyspace và table, không cần script riêng
2. **Modular Design**: Functions cho từng task (HDFS, Spark, Transform)
3. **Better Error Handling**: Try-catch cho từng file, không dừng nếu 1 file lỗi
4. **Progress Tracking**: Hiển thị progress mỗi 100 files
5. **Summary Report**: Tổng kết success/failed counts

### ML Prediction Script (`predict_prices.py`):
1. **No Model Save/Load**: Train và predict trong cùng session - tránh lỗi Windows
2. **Spark UI Integration**: Web UI available at http://localhost:4040
3. **Keep Session Alive**: Press ENTER to keep UI accessible
4. **Complete Statistics**: Breakdowns by city, home type, overall stats
5. **Windows Optimized**: Local temp dir, clean shutdown, no errors

### Version Compatibility:
- Downgrade từ PySpark 4.0.1 → 3.5.0 để tương thích với Cassandra connector
- Sử dụng Scala 2.12 connector thay vì 2.13
- Python 3.11 cho full cassandra-driver support

---

## 🗑️ Cleanup Guide

Xem file `CLEANUP.md` để biết cách xóa hoàn toàn project và tất cả data.

**Quick cleanup:**
```bash
# Stop all services
docker-compose down -v

# Delete external storage (OUTSIDE project - IMPORTANT!)
cd D:\Workspace\
rmdir /s /q storage

# Delete project
rmdir /s /q bigdata-project-20251

# Clean Spark temp
cd C:\Users\binht\AppData\Local\Temp\
for /d %i in (spark-*) do rmdir /s /q "%i"
```

**Total space freed**: ~11-22 GB

---

## 👥 Nhóm thực hiện

**Nhóm 14 - Lớp 154050**

---

## 📝 License

Dự án học tập - Đại học [Tên trường]

---

## 📞 Liên hệ

Nếu có vấn đề, vui lòng tạo issue trong repository hoặc liên hệ nhóm.

---

## 🎓 Learning Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Cassandra Documentation](https://cassandra.apache.org/doc/)
- [HDFS Architecture](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Spark-Cassandra Connector](https://github.com/apache/cassandra-spark-connector)
- [Random Forest Algorithm](https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-regression)
