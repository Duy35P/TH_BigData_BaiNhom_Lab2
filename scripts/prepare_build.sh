#!/usr/bin/env bash
set -e

echo "=== Chuẩn bị môi trường build cho spark-hdfs-lab:1.0 ==="

# 1. Tải Hadoop 3.3.6 nếu chưa có
if [ ! -f "hadoop-3.3.6.tar.gz" ]; then
  echo "[1/2] Đang tải hadoop-3.3.6.tar.gz từ Apache Archive..."
  curl -fL "https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz" -o hadoop-3.3.6.tar.gz
else
  echo "[1/2] Đã có sẵn hadoop-3.3.6.tar.gz."
fi

# 2. Tạo thư mục spark-pip từ gói pyspark==3.5.1 nếu chưa có
if [ ! -d "spark-pip/bin" ]; then
  echo "[2/2] Đang trích xuất Spark 3.5.1 từ gói pip pyspark==3.5.1 vào thư mục spark-pip/..."
  python3 -m pip install --quiet --target ./_tmp_pyspark pyspark==3.5.1
  mkdir -p spark-pip
  cp -r ./_tmp_pyspark/pyspark/* spark-pip/
  rm -rf ./_tmp_pyspark
  chmod +x spark-pip/bin/* spark-pip/sbin/* 2>/dev/null || true
else
  echo "[2/2] Đã có sẵn thư mục spark-pip/."
fi

echo "=== Hoàn tất! Bây giờ bạn có thể chạy: docker compose build ==="
