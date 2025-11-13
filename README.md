[Báo cáo BTL BigData - Nhóm 14 - Lớp 154050](https://docs.google.com/document/d/1Svi3nbpFZvkNQm9AJzbJgJ29YFHlan6-cYaCBs4nb8U/edit?tab=t.0)

# Zillow House with Kafka, Spark, and Cassandra

## **Mô tả dự án**
Dự án này xây dựng một pipeline dữ liệu hoàn chỉnh để dự đoán giá nhà dựa trên dữ liệu bất động sản. Hệ thống bao gồm các bước:
1. **Ingest**: Tạo dữ liệu giả lập và gửi vào Kafka.
2. **Processing**: Xử lý dữ liệu với Spark Streaming và Spark MLlib.
3. **Storage**: Lưu trữ dữ liệu trong HDFS, Cassandra, và Elasticsearch.
4. **Prediction**: Huấn luyện mô hình Random Forest Regressor để dự đoán giá nhà.

---

## **Kiến trúc hệ thống**

### **1. Producer**
- Sinh dữ liệu bất động sản giả lập với các thuộc tính:
  - `price`, `city`, `hometype`, `bedrooms`, `bathrooms`,...
- Gửi dữ liệu đến Kafka topic `example_topic`.

### **2. Batch Consumer**
- Lưu dữ liệu từ Kafka vào HDFS theo batch để phục vụ xử lý sau.

### **3. Structured Streaming Consumer**
- Xử lý dữ liệu thời gian thực từ Kafka.
- Lưu dữ liệu vào Elasticsearch để phân tích và tìm kiếm.

### **4. Machine Learning Pipeline**
- Dữ liệu được tải từ Cassandra.
- Sử dụng **Random Forest Regressor** để dự đoán giá nhà dựa trên các thuộc tính.

---

## **Hướng dẫn cài đặt**

### **1. Yêu cầu hệ thống**
- **Python** >= 3.8
- **Java** >= 8
- **Apache Kafka**
- **Apache Spark** >= 3.2.0
- **Elasticsearch** >= 8.x
- **Cassandra** >= 3.x
- **Docker Compose** (nếu cần)

### **2. Cài đặt thư viện**
Chạy lệnh sau để cài đặt các thư viện cần thiết:
```bash
pip install pyspark kafka-python cassandra-driver hdfs
```

### **3. Hướng dẫn sử dụng**

**Chạy Kafka và Cassandra**
Nếu sử dụng Docker Compose, chạy:
```bash
docker-compose up -d
```
**Producer**
Chạy file producer.py để sinh dữ liệu và gửi vào Kafka:
```bash
python producer.py
```

**Batch Consumer**
Lưu dữ liệu từ Kafka vào HDFS:
```bash
python consumer_batch.py
```

**Structured Streaming Consumer**
Xử lý dữ liệu và lưu vào Elasticsearch:
```bash
python consumer_structured_stream.py
```

**Huấn luyện mô hình dự đoán giá nhà**
Chạy file sparkML.py.py:
```bash
python sparkML.py
```
