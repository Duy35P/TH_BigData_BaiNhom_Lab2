# Dự án: Triển khai cụm Spark và Quản lý dữ liệu trên HDFS bằng Docker Compose

Đồ án thực hành môn **Công nghệ Dữ liệu lớn (Big Data)**: Xây dựng cụm tính toán phân tán gồm **Apache Hadoop 3.3.6 (HDFS + YARN)** và **Apache Spark 3.5.1 (Spark Standalone + Spark on YARN + Spark History Server)** trên nền tảng **Docker Compose**.

---

## 1. Cấu trúc thư mục dự án

```text
spark-hdfs-lab/
├── docker-compose.yml              # Định nghĩa 11 container theo 3 profiles (standalone, yarn, history)
├── Dockerfile                      # Cấu hình build image spark-hdfs-lab:1.0
├── .dockerignore                   # Loại trừ dữ liệu không cần thiết khi build Docker
├── .gitignore                      # Loại trừ bộ cài nặng (hadoop-3.3.6.tar.gz, spark-pip/, data/)
├── README.md                       # Hướng dẫn cài đặt và sử dụng cho người nhận
├── AGENTS.md / CLAUDE.md           # Kế hoạch 5 buổi thực hành và ghi chú hệ thống
├── BAO_CAO_DO_AN_BIGDATA.docx      # Báo cáo đồ án hoàn chỉnh định dạng chuẩn Word (.docx)
├── BAO_CAO_DO_AN.md                # Báo cáo đồ án định dạng Markdown
├── conf/
│   ├── hadoop/                     # core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml
│   └── spark/                      # spark-defaults.conf, spark-env.sh
├── scripts/
│   ├── entrypoint.sh               # Kịch bản khởi động từng vai trò container
│   └── prepare_build.sh            # Kịch bản tự động tải Hadoop 3.3.6 & trích xuất Spark 3.5.1 để build
├── apps/
│   ├── pi.py                       # Ước lượng số Pi bằng Monte Carlo theo số lượng Partition
│   ├── wordcount.py                # Đếm tần suất từ trên movies.csv và lưu kết quả lên HDFS
│   └── top10_movies.py             # Phân tích Top 10 phim điểm trung bình cao nhất (>=50 đánh giá), xuất Parquet
├── data/
│   ├── movies.csv                  # Danh mục phim MovieLens (~3 MB)
│   └── ratings_1m.csv              # Tập dữ liệu 1 triệu lượt đánh giá (~25 MB)
└── screenshots/                    # Bộ 32 hình ảnh minh chứng kết quả từ Buổi 1 đến Buổi 4 (A0 -> D6)
```

---

## 2. Hướng dẫn chạy nhanh cho người nhận

Bạn có thể khởi chạy cụm theo **1 trong 2 cách** dưới đây:

### Cách A: Dùng file Docker Image đóng gói sẵn (`spark-hdfs-lab-image.tar.gz`) — Chạy ngay trong 10 giây (Không cần build)
Nếu bạn nhận kèm file `spark-hdfs-lab-image.tar.gz`:
```bash
# 1. Nạp Docker image vào máy
docker load -i spark-hdfs-lab-image.tar.gz

# 2. Khởi chạy toàn bộ cụm (HDFS + Spark Standalone + YARN + History Server)
docker compose --profile standalone --profile yarn --profile history up -d

# 3. Kiểm tra trạng thái các container
docker compose ps
```

### Cách B: Tự build Docker Image từ mã nguồn (`Dockerfile`)
Nếu bạn chỉ tải gói mã nguồn gọn nhẹ (`spark-hdfs-lab-package.zip` hoặc clone từ GitHub):
```bash
# 1. Chạy script tự động chuẩn bị bộ cài hadoop-3.3.6.tar.gz và thư mục spark-pip/
bash scripts/prepare_build.sh

# 2. Build image spark-hdfs-lab:1.0
docker compose build

# 3. Khởi chạy cụm
docker compose --profile standalone --profile yarn up -d
```

---

## 3. Danh sách cổng giao diện Web UI (Mở trên trình duyệt)

| Dịch vụ | Địa chỉ Web UI | Mô tả |
| :--- | :--- | :--- |
| **HDFS NameNode** | `http://localhost:9870` | Quản lý HDFS, xem Live DataNodes (2) & duyệt File Explorer |
| **HDFS DataNode 1, 2** | `http://localhost:9864`, `http://localhost:9865` | Thông tin chi tiết từng DataNode |
| **YARN ResourceManager** | `http://localhost:8088` | Quản lý cụm YARN, Nodes (2 NodeManagers) và hàng đợi Jobs |
| **YARN NodeManager 1, 2** | `http://localhost:8042`, `http://localhost:8043` | Giám sát Container trên từng NodeManager |
| **Spark Standalone Master** | `http://localhost:8080` | Quản lý cụm Spark Standalone (2 Workers, 4 Cores, 2 GiB RAM) |
| **Spark Worker 1, 2** | `http://localhost:8081`, `http://localhost:8082` | Giao diện chi tiết từng Spark Worker |
| **Spark History Server** | `http://localhost:18080` | Xem lại DAG và lịch sử các ứng dụng đã kết thúc (`/spark-logs`) |
| **Spark Application UI** | `http://localhost:4040` | Xem trực tiếp DAG Visualization khi job đang chạy trên `client` |

---

## 4. Các lệnh thực hành tiêu biểu

### 4.1. Thao tác quản lý dữ liệu trên HDFS
```bash
# Tạo thư mục và đẩy dữ liệu từ container client lên HDFS
docker compose exec client hdfs dfs -mkdir -p /bigdata/input /spark-logs
docker compose exec client hdfs dfs -put -f /data/movies.csv /bigdata/input/
docker compose exec client hdfs dfs -put -f /data/ratings_1m.csv /bigdata/input/

# Kiểm tra danh sách file và cấu trúc Block + vị trí DataNode
docker compose exec client hdfs dfs -ls -h /bigdata/input
docker compose exec client hdfs fsck /bigdata/input/ratings_1m.csv -files -blocks -locations
```

### 4.2. Chạy ứng dụng PySpark trên Spark Standalone
```bash
# 1. Tính số Pi với 100 partitions
docker compose exec client spark-submit --master spark://spark-master:7077 /apps/pi.py 100

# 2. Đếm tần suất từ (WordCount) trên movies.csv
docker compose exec client spark-submit --master spark://spark-master:7077 /apps/wordcount.py

# 3. Phân tích Top 10 phim có điểm trung bình cao nhất (>= 50 lượt đánh giá) và lưu Parquet
docker compose exec client spark-submit --master spark://spark-master:7077 --executor-memory 512m /apps/top10_movies.py
```

### 4.3. Chạy ứng dụng PySpark trên Hadoop YARN (Client Mode & Cluster Mode)
```bash
# Tối ưu hóa (chỉ cần chạy 1 lần đầu): Đẩy bộ thư viện jar của Spark lên HDFS /spark-jars
docker compose exec client hdfs dfs -mkdir -p /spark-jars
docker compose exec client bash -c "hdfs dfs -put -f /opt/spark/jars/*.jar /spark-jars/"

# 1. Chạy ở chế độ YARN Client Mode
docker compose exec client spark-submit --master yarn --deploy-mode client --executor-memory 512m --num-executors 2 /apps/wordcount.py

# 2. Chạy ở chế độ YARN Cluster Mode và lấy kết quả qua yarn logs
docker compose exec client spark-submit --master yarn --deploy-mode cluster --driver-memory 512m --executor-memory 512m --num-executors 2 /apps/top10_movies.py
docker compose exec client yarn logs -applicationId <application_id>
```

### 4.4. Tắt cụm (Bảo toàn dữ liệu HDFS trên Docker Volume)
```bash
docker compose --profile standalone --profile yarn --profile history down
```
*(Lưu ý: Không thêm cờ `-v` khi chạy `down` để giữ nguyên dữ liệu HDFS trong các volume `nn-data`, `dn1-data`, `dn2-data`).*
