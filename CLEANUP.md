# Complete Cleanup Guide

This guide shows you where ALL data from this project is stored and how to clean it up completely.

---

## 📍 Data Storage Locations

### 1. **Project Directory** (Main folder)
```
D:\Workspace\bigdata-project-20251\
```

**Contains:**
- Source code
- Configuration files
- Virtual environment (.venv/)
- Hadoop binaries (hadoop/bin/)

**Size:** ~500 MB - 2 GB (depending on .venv)

---

### 2. **Docker Volumes - External Storage** ⚠️ IMPORTANT
```
D:\Workspace\storage\
├── hadoop/          # HDFS data (namenode + 3 datanodes)
│   ├── namenode/
│   ├── datanode-1/
│   ├── datanode-2/
│   └── datanode-3/
└── cassandra/       # Cassandra database data
```

**Contains:**
- All HDFS files (JSON batch files from Kafka)
- All Cassandra data (processed real estate data)
- **This is the LARGEST storage** (can be several GB)

⚠️ **Critical:** This folder is OUTSIDE the project directory!

---

### 3. **Docker Internal Volumes**
Docker also creates internal volumes. Check with:
```bash
docker volume ls
```

**Contains:**
- Kafka data
- Zookeeper data
- Elasticsearch data
- Spark logs

---

### 4. **System Temp Files**

#### Windows:
```
C:\Users\binht\AppData\Local\Temp\spark-*
```

**Contains:**
- Temporary Spark execution files
- Each run creates a new `spark-<uuid>` folder
- Usually auto-cleaned, but can accumulate

---

### 5. **Ivy/Maven Cache** (Spark dependencies)

#### Windows:
```
C:\Users\binht\.ivy2\
├── cache/    # Downloaded dependency cache
└── jars/     # Compiled JARs
```

**Contains:**
- Cassandra Spark Connector (3.5.0)
- Java Driver dependencies
- Scala libraries

**Size:** ~100-200 MB

**Note:** Can be safely deleted. Will re-download on next Spark run.

---

## 🗑️ Complete Cleanup Steps

### Step 1: Stop All Running Services

#### Windows:
```bash
# Stop all Python processes
.\stop.bat

# Or manually
# Press Ctrl+C in each terminal running producer/consumer
```

#### Linux/Mac:
```bash
./stop.sh
```

---

### Step 2: Stop and Remove Docker Containers

```bash
# Stop all containers
docker-compose down

# Remove all containers, networks, and volumes
docker-compose down -v

# Optional: Remove all unused Docker volumes
docker volume prune -f
```

---

### Step 3: Delete External Storage (OUTSIDE project)

⚠️ **WARNING:** This deletes ALL your HDFS and Cassandra data!

#### Windows:
```bash
# From project directory
cd D:\Workspace\

# Remove storage folder
rmdir /s /q storage
```

#### Linux/Mac:
```bash
cd /path/to/workspace/

rm -rf storage
```

**What's deleted:**
- All HDFS data (~several GB)
- All Cassandra data
- All batch files from Kafka

---

### Step 4: Delete Project Directory

#### Windows:
```bash
cd D:\Workspace\
rmdir /s /q bigdata-project-20251
```

#### Linux/Mac:
```bash
cd /path/to/workspace/
rm -rf bigdata-project-20251
```

---

### Step 5: Clean System Temp Files (Optional)

#### Windows:
```bash
# Clean Spark temp files
cd C:\Users\binht\AppData\Local\Temp\
for /d %i in (spark-*) do rmdir /s /q "%i"
```

#### Linux/Mac:
```bash
# Clean Spark temp files
rm -rf /tmp/spark-*
```

---

### Step 6: Clean Ivy/Maven Cache (Optional)

⚠️ **Warning:** This removes ALL Spark dependencies. They will re-download on next use.

#### Windows:
```bash
# Remove Ivy cache
rmdir /s /q C:\Users\binht\.ivy2

# Or keep cache but remove old versions
rmdir /s /q C:\Users\binht\.ivy2\cache
rmdir /s /q C:\Users\binht\.ivy2\jars
```

#### Linux/Mac:
```bash
rm -rf ~/.ivy2
```

---

## 📊 Disk Space Summary

### Before Cleanup:
```
Project Directory:           ~2 GB
External Storage (HDFS):     ~5-10 GB (depends on runtime)
External Storage (Cassandra): ~1-3 GB
Docker Volumes:              ~2-5 GB
System Temp (Spark):         ~500 MB - 2 GB
Ivy Cache:                   ~200 MB

TOTAL: ~11-22 GB
```

### After Complete Cleanup:
```
All data deleted: 0 bytes
```

---

## 🔄 Quick Cleanup Script

### Windows - Create `cleanup.bat`:
```batch
@echo off
echo ========================================
echo Complete Cleanup - Bigdata Project
echo ========================================
echo.

echo [1/6] Stopping Docker containers...
docker-compose down -v

echo [2/6] Deleting external storage...
cd D:\Workspace\
rmdir /s /q storage

echo [3/6] Deleting project directory...
rmdir /s /q bigdata-project-20251

echo [4/6] Cleaning Spark temp files...
cd C:\Users\binht\AppData\Local\Temp\
for /d %%i in (spark-*) do rmdir /s /q "%%i"

echo [5/6] Cleaning Ivy cache...
rmdir /s /q C:\Users\binht\.ivy2

echo [6/6] Pruning Docker volumes...
docker volume prune -f

echo.
echo ========================================
echo Cleanup completed!
echo ========================================
```

### Linux/Mac - Create `cleanup.sh`:
```bash
#!/bin/bash

echo "========================================"
echo "Complete Cleanup - Bigdata Project"
echo "========================================"
echo

echo "[1/6] Stopping Docker containers..."
docker-compose down -v

echo "[2/6] Deleting external storage..."
cd ..
rm -rf storage

echo "[3/6] Deleting project directory..."
rm -rf bigdata-project-20251

echo "[4/6] Cleaning Spark temp files..."
rm -rf /tmp/spark-*

echo "[5/6] Cleaning Ivy cache..."
rm -rf ~/.ivy2

echo "[6/6] Pruning Docker volumes..."
docker volume prune -f

echo
echo "========================================"
echo "Cleanup completed!"
echo "========================================"
```

---

## 🎯 Selective Cleanup (Keep Docker Images)

If you want to keep Docker images for faster restart:

```bash
# Only remove containers and volumes, keep images
docker-compose down -v

# Delete external storage
cd D:\Workspace\
rmdir /s /q storage

# Delete project
rmdir /s /q bigdata-project-20251

# Clean temp files
cd C:\Users\binht\AppData\Local\Temp\
for /d %i in (spark-*) do rmdir /s /q "%i"
```

This saves ~2-3 GB of Docker images (Kafka, Hadoop, Cassandra, etc.)

---

## 📝 What to Keep vs Delete

### ✅ Safe to Delete (Can re-download):
- External storage folder (`D:\Workspace\storage\`)
- Spark temp files
- Ivy cache
- Docker volumes

### ⚠️ Cannot Recover After Delete:
- Your custom code changes (if not committed to git)
- Collected HDFS data (batch files)
- Cassandra data (processed results)

### 💡 Best Practice:
Before cleanup, commit your code:
```bash
git add .
git commit -m "Final version"
git push
```

---

## 🔍 Verify Cleanup

After cleanup, verify everything is removed:

```bash
# Check Docker
docker ps -a
docker volume ls

# Check storage
ls D:\Workspace\storage\     # Should not exist

# Check project
ls D:\Workspace\bigdata-project-20251\  # Should not exist

# Check Spark temp
ls C:\Users\binht\AppData\Local\Temp\spark-*  # Should be empty
```

---

## 🚀 Fresh Start After Cleanup

To start fresh:

```bash
# Clone project again
git clone <repository-url>
cd bigdata-project-20251

# Create venv and install
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Start Docker
docker-compose up -d

# External storage will be auto-created
# Ivy cache will be auto-downloaded on first Spark run
```

---

## 📞 Need Help?

If you have issues with cleanup or leftover files, check:
1. Task Manager (Windows) / Activity Monitor (Mac) for running processes
2. Docker Desktop for remaining containers/volumes
3. Disk Cleanup utility for system temp files
