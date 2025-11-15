# 🚀 HƯỚNG DẪN NHANH - QUICK START

## ⚡ Chạy dự án trong 5 phút

### Windows:

```bash
# 1. Clone project và vào thư mục
git clone <repo-url>
cd bigdata-project-20251

# 2. Tạo virtual environment với Python 3.11 và cài đặt
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Lưu ý: Đảm bảo bạn đã cài Python 3.11
# Download tại: https://www.python.org/downloads/release/python-31111/

# 3. Khởi động Docker
docker-compose up -d

# 4. Đợi 60 giây, sau đó chạy script tự động
.\start.bat

# 5. Để dừng hệ thống
.\stop.bat
```

### Linux/Mac:

```bash
# 1. Clone project và vào thư mục
git clone <repo-url>
cd bigdata-project-20251

# 2. Tạo virtual environment và cài đặt
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Khởi động Docker
docker-compose up -d

# 4. Đợi 60 giây, sau đó chạy script tự động
chmod +x start.sh
./start.sh

# 5. Để dừng hệ thống
chmod +x stop.sh
./stop.sh
```

---

## 📊 Xem kết quả

Sau khi chạy `start.bat` hoặc `start.sh`, bạn sẽ thấy 3 cửa sổ terminal mới:

### Terminal 1 - Kafka Producer:
```
Sent data: {'city': 'Los Angeles', 'price': 7578356, ...}
Sent data: {'city': 'Beverly Hills', 'price': 8109473, ...}
```

### Terminal 2 - Batch Consumer:
```
INFO - Batch of 10 messages saved to /data/kafka_messages\2025\11\15\...
```

### Terminal 3 - Spark Streaming:
```
+-------------------+--------------+-------------+
|window             |city          |average_price|
+-------------------+--------------+-------------+
|...                |Los Angeles   |6022806.0    |
|...                |Beverly Hills |8109473.0    |
+-------------------+--------------+-------------+
```

---

## 🌐 Web UIs

Truy cập các giao diện web:

- **Spark Master**: http://localhost:8080
- **HDFS**: http://localhost:9870
- **Kibana**: http://localhost:5601

---

## ❌ Gặp lỗi?

### Lỗi: "Docker is not running"
→ Khởi động Docker Desktop

### Lỗi: "Port already in use"
→ Dừng các process đang dùng port:
```bash
# Windows
netstat -ano | findstr :9092
taskkill /PID <PID> /F

# Linux/Mac
lsof -i :9092
kill -9 <PID>
```

### Lỗi: "ModuleNotFoundError"
→ Cài lại dependencies:
```bash
pip install -r requirements.txt
```

---

## 🤖 Machine Learning (Tùy chọn)

Sau khi pipeline chạy được 10-15 phút:

```bash
# 1. Load data từ HDFS vào Cassandra
python spark/batch_processing.py

# 2. Kiểm tra Cassandra có đủ data chưa
python check_cassandra_data.py

# 3. Train ML model với dữ liệu từ Cassandra
python spark/sparkML.py
```

---

## 📖 Đọc thêm

Chi tiết đầy đủ xem tại: [README.md](README.md)
