@echo off
echo ========================================
echo Zillow Real Estate Data Pipeline
echo Nhom 14 - Lop 154050
echo ========================================
echo.

REM Check if virtual environment exists
if not exist ".venv" (
    echo [ERROR] Virtual environment not found!
    echo Please run: python -m venv .venv
    echo Then activate it and install dependencies: pip install -r requirements.txt
    pause
    exit /b 1
)

REM Activate virtual environment
echo [1/6] Activating virtual environment...
call .venv\Scripts\activate.bat

REM Check if Docker is running
echo.
echo [2/6] Checking Docker services...
docker ps >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not running! Please start Docker Desktop.
    pause
    exit /b 1
)

REM Start Docker services
echo.
echo [3/6] Starting Docker Compose services...
docker-compose up -d

REM Wait for services to be ready
echo.
echo [4/6] Waiting for services to start (60 seconds)...
timeout /t 60 /nobreak

REM Check services status
echo.
echo [5/6] Checking service status...
docker-compose ps

REM Start all components
echo.
echo [6/6] Starting Kafka Producer, Batch Consumer, and Spark Streaming...
echo.
echo ========================================
echo Opening 3 new terminals:
echo   - Terminal 1: Kafka Producer
echo   - Terminal 2: Batch Consumer (HDFS)
echo   - Terminal 3: Spark Streaming
echo ========================================
echo.

REM Start Producer in new window
start "Kafka Producer" cmd /k "call .venv\Scripts\activate.bat && python kafka\producer.py"

REM Wait a bit before starting consumers
timeout /t 5 /nobreak

REM Start Batch Consumer in new window
start "Batch Consumer - HDFS" cmd /k "call .venv\Scripts\activate.bat && python kafka\consumer_batch.py"

REM Wait a bit before starting streaming
timeout /t 5 /nobreak

REM Start Spark Streaming in new window
start "Spark Streaming" cmd /k "call .venv\Scripts\activate.bat && python kafka\consumer_structured_stream.py"

echo.
echo ========================================
echo All components started successfully!
echo ========================================
echo.
echo Web UIs available at:
echo   - Spark Master: http://localhost:8080
echo   - Spark Worker: http://localhost:8081
echo   - HDFS NameNode: http://localhost:9870
echo   - Elasticsearch: http://localhost:9200
echo   - Kibana: http://localhost:5601
echo.
echo To stop all services:
echo   1. Close all terminal windows (Ctrl+C)
echo   2. Run: docker-compose down
echo.
pause
