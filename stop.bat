@echo off
echo ========================================
echo Stopping Zillow Data Pipeline
echo ========================================
echo.

echo [1/2] Stopping Docker Compose services...
docker-compose down

echo.
echo [2/2] All services stopped.
echo.
echo Note: Close all running terminal windows manually.
echo       (Kafka Producer, Batch Consumer, Spark Streaming)
echo.
pause
