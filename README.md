# Zillow Real Estate Data Pipeline - Nhóm 14

[Báo cáo BTL BigData - Nhóm 14 - Lớp 154050](https://docs.google.com/document/d/1Svi3nbpFZvkNQm9AJzbJgJ29YFHlan6-cYaCBs4nb8U/edit?tab=t.0)

## 📋 Mô tả dự án

Hệ thống xử lý dữ liệu bất động sản real-time sử dụng công nghệ Big Data. Pipeline bao gồm:

1. **Data Ingestion**: Tạo dữ liệu giả lập nhà đất California và gửi vào Kafka
2. **Stream Processing**: Xử lý real-time với Spark Structured Streaming
3. **Batch Processing**: Lưu trữ batch data vào HDFS
4. **Storage**: Phân tán dữ liệu trên Cassandra và Elasticsearch
5. **Analytics**: Aggregation theo thành phố, thời gian

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
        Cassandra → ML Training (sparkML.py)
```

### Các thành phần:

- **Kafka Producer**: Sinh dữ liệu giả lập nhà đất (price, bedrooms, city, ...)
- **Kafka Cluster**: 3 brokers (ports 9092, 9093, 9094) + ZooKeeper
- **Spark Streaming**: Xử lý real-time, tính toán metrics theo time window
- **Batch Consumer**: Đọc từ Kafka, lưu batch vào HDFS
- **HDFS**: Lưu trữ phân tán dữ liệu batch
- **Batch Processing**: Load data từ HDFS vào Cassandra
- **Cassandra**: NoSQL database cho ML pipeline
- **ML Training**: Huấn luyện model dự đoán giá nhà (Random Forest)
- **Elasticsearch + Kibana**: Search và visualization (reserved)

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
- ✅ PySpark 4.0.1 stable và không có worker crashes
- ✅ Tương thích với toàn bộ Big Data stack

**Lưu ý**: Python 3.13 chưa được cassandra-driver hỗ trợ. Sử dụng Python 3.11 để tránh vấn đề tương thích.

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
pip install -r requirements.txt
```

#### Linux/Mac:
```bash
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

### Machine Learning:

**Lưu ý**: sparkML.py đọc dữ liệu từ Cassandra để train model dự đoán giá nhà.

ML training workflow bao gồm các bước sau:

#### Bước 1: Chạy pipeline để collect data (đang chạy từ start.bat/start.sh)
```bash
# Producer, Batch Consumer, và Spark Streaming đang chạy
# Đợi 10-15 phút để data được collect vào HDFS
```

#### Bước 2: Load data từ HDFS vào Cassandra
```bash
python spark/batch_processing.py
```

#### Bước 3: Kiểm tra Cassandra có đủ data chưa
```bash
python check_cassandra_data.py
```

Script này sẽ:
- Kết nối tới Cassandra
- Đếm số lượng records trong table `finaldata1.data2`
- Hiển thị sample data
- Đưa ra khuyến nghị có nên train ML model hay chưa

**Khuyến nghị số lượng data:**
- Tối thiểu: 100-200 rows
- Tối ưu: 1000+ rows

#### Bước 4: Train ML model với dữ liệu thực từ Cassandra
```bash
# Train model dự đoán giá nhà với Random Forest
python spark/sparkML.py
```

### Dừng hệ thống:

```bash
# Nhấn Ctrl+C ở mỗi terminal để dừng các consumer/producer

# Dừng Docker services
docker-compose down
```

---

## 📊 Kết quả mong đợi

### Producer Output:
```
Sent data: {'timestamp': 1763174687104, 'zpid': 336424216, 'city': 'Los Angeles', 'price': 7578356, ...}
Sent data: {'timestamp': 1763174687952, 'zpid': 261109533, 'city': 'Glendale', 'price': 7632629, ...}
```

### Batch Consumer Output:
```
2025-11-15 10:06:05,815 - __main__ - INFO - Batch of 10 messages saved to /data/kafka_messages\2025\11\15\10_06_05_843515_batch.json
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

## 🔧 Các lỗi đã fix và giải pháp

### 1. ❌ Lỗi kafka-python module
**Lỗi**: `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`

**Nguyên nhân**: Package `kafka-python` không còn được maintain

**Giải pháp**: Thay bằng `kafka-python-ng`
```bash
pip uninstall kafka-python -y
pip install kafka-python-ng
```

### 2. ❌ Lỗi Kafka Docker
**Lỗi**: `KAFKA_PROCESS_ROLES is not set`

**Nguyên nhân**: Kafka image `latest` mặc định dùng KRaft mode

**Giải pháp**: Đổi sang version `7.4.0` hỗ trợ ZooKeeper
```yaml
image: confluentinc/cp-kafka:7.4.0
```

### 3. ❌ Lỗi PySpark typing
**Lỗi**: `ModuleNotFoundError: No module named 'typing.io'`

**Nguyên nhân**: Python 3.13 không tương thích với PySpark 3.3.2

**Giải pháp**: Upgrade PySpark lên 4.0.1
```bash
pip install --upgrade pyspark
```

### 4. ❌ Lỗi HADOOP_HOME
**Lỗi**: `HADOOP_HOME and hadoop.home.dir are unset`

**Nguyên nhân**: Windows cần Hadoop binaries

**Giải pháp**: Tự động download winutils.exe và set HADOOP_HOME trong code

### 5. ❌ Lỗi Kafka API Version
**Lỗi**: `NoBrokersAvailable` với consumer_batch.py

**Nguyên nhân**: Timeout khi detect API version

**Giải pháp**: Set explicit API version
```python
api_version=(2, 8, 1),
request_timeout_ms=30000
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
│   ├── batch_processing.py          # Spark batch processing → Cassandra
│   ├── sparkML.py                   # ML model training
│   └── sparkML_note.txt             # Python compatibility notes
├── hadoop/
│   └── bin/                         # Hadoop binaries (auto-downloaded)
├── check_cassandra_data.py          # Check Cassandra data before ML
├── docker-compose.yml               # Docker services config
├── requirements.txt                 # Python dependencies
├── start.bat                        # Windows startup script
├── start.sh                         # Linux/Mac startup script
├── stop.bat                         # Windows stop script
├── stop.sh                          # Linux/Mac stop script
├── QUICKSTART.md                    # Quick start guide
└── README.md                        # File này
```

---

## 🌐 Web UIs

Sau khi khởi động Docker services, có thể truy cập:

- **Spark Master**: http://localhost:8080
- **Spark Worker**: http://localhost:8081
- **HDFS NameNode**: http://localhost:9870
- **Elasticsearch**: http://localhost:9200
- **Kibana**: http://localhost:5601

---

## 📦 Dependencies chính

```
pyspark==4.0.1
kafka-python-ng
hdfs
cassandra-driver
pandas
numpy
```

---

## 🐛 Troubleshooting

### Docker containers không start:
```bash
docker-compose down
docker-compose up -d
```

### Port đã được sử dụng:
Kiểm tra và kill process đang dùng port:
```bash
# Windows
netstat -ano | findstr :9092
taskkill /PID <PID> /F

# Linux/Mac
lsof -i :9092
kill -9 <PID>
```

### Kafka connection timeout:
Đợi thêm 30-60 giây để Kafka khởi động hoàn toàn.

---

## 👥 Nhóm thực hiện

**Nhóm 14 - Lớp 154050**

---

## 📝 License

Dự án học tập - Đại học [Tên trường]

---

## 📞 Liên hệ

Nếu có vấn đề, vui lòng tạo issue trong repository hoặc liên hệ nhóm.
