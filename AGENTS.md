# Dự án: Cụm Spark + HDFS bằng Docker Compose

Đề tài: "Triển khai cụm Spark và quản lý dữ liệu trên HDFS" (Docker Compose, HDFS,
Spark Standalone, Spark on YARN). Mỗi bài tập cần ví dụ, hướng dẫn và ảnh chụp kết quả.

## Môi trường
- Windows 11 + WSL2, distro `Ubuntu-26.04`, user `dell`, project `/home/dell/spark-hdfs-lab`.
- Lệnh từ Windows: `wsl -d Ubuntu-26.04 -u dell -- bash -lc "cd ~/spark-hdfs-lab && <lệnh>"`.
- Docker Desktop (engine trên Windows, tích hợp WSL). Image `spark-hdfs-lab:1.0`:
  Ubuntu 22.04, Java 11, Hadoop 3.3.6, Spark 3.5.1 (bản pip copy vào `spark-pip/`, không có
  jar `examples`, nên SparkPi dùng file `pi.py` tự viết).
- RAM WSL 10 GB (`C:\Users\Dell\.wslconfig`). Chạy theo profile để tiết kiệm RAM.

## Bố cục
- `docker-compose.yml`, `Dockerfile`, `scripts/entrypoint.sh`
- `conf/hadoop/*.xml`, `conf/spark/{spark-defaults.conf,spark-env.sh}`
- `apps/` -> `/apps` trong container `client`; `data/` -> `/data` (movies.csv, ratings_1m.csv)
- `/home/dell/movielens_data/ml-25m` -> `/ml25m` (chỉ đọc; ratings.csv 647 MB)
- `screenshots/` ảnh kết quả, đặt tên theo mã: `A1_docker_ps.png`, `A2_namenode.png`, `B1_...`, `C1_...`, `D1_...`

## Lệnh thường dùng (chạy trong ~/spark-hdfs-lab)
- Bật: `docker compose --profile standalone --profile yarn up -d`
  (thêm `--profile history` khi cần History Server)
- Tắt, giữ dữ liệu: `docker compose --profile standalone --profile yarn --profile history down`
- Vào client: `docker compose exec client bash`
- HDFS: `docker compose exec client hdfs dfs -ls /`
- Submit Standalone: `spark-submit --master spark://spark-master:7077 /apps/<file>.py`
- Submit YARN: `spark-submit --master yarn --deploy-mode client|cluster /apps/<file>.py`
- Sửa file trong `conf/` hoặc Dockerfile: `docker compose build` rồi `up -d --force-recreate`.

## Cổng (từ Windows)
NameNode 9870, DataNode 9864/9865, ResourceManager 8088, NodeManager 8042/8043,
Spark Master 8080, Worker 8081/8082, History 18080, Spark app UI 4040/4041.

## Cấu hình quan trọng
- HDFS: replication 2, block 128 MB, `hdfs://namenode:9000`.
- YARN: mỗi NodeManager 2048 MB / 2 vcores (tổng 4096 MB), min allocation 256 MB. Executor 512 MB cộng overhead
  khoảng 1024 MB, nên cụm chỉ chạy được khoảng 3–4 container cùng lúc.
- Spark: event log ở `hdfs://namenode:9000/spark-logs`. **Thư mục này phải tồn tại trước khi
  submit**: `hdfs dfs -mkdir -p /spark-logs`.
- Standalone không hỗ trợ cluster mode cho Python. Cluster mode chỉ so sánh đầy đủ trên YARN.

## KHÔNG làm
- Không chạy `docker compose down -v` hoặc `docker volume rm` (xóa dữ liệu HDFS).
- Không sửa `/usr/local/hadoop`, `~/.bashrc`, hay thư mục cũ trong `/home/dell`
  (`movie_env`, `kafka_*`, `airflow`, ...). Đó là dự án cũ, không thuộc đề tài.
- Không chạy `wsl --terminate/--shutdown` khi Docker Desktop đang mở.
- Không đưa `spark-pip/`, `hadoop-3.3.6.tar.gz`, `data/` vào git.
- Không chạy Hadoop/Spark cũ cùng lúc với cụm Docker (tranh cổng 9870, 8088, 9000).

---

## Kế hoạch 5 buổi thực hiện

Mỗi buổi bắt đầu bằng lệnh mở lại cụm theo profile cần dùng và kết thúc bằng lệnh `down`.

| Buổi | Nội dung | Thời gian | Ảnh |
| :---: | :--- | :--- | :--- |
| 1 | Hoàn tất Giai đoạn 1 | 45 phút | A1–A4, BT12, BT13 |
| 2 | Quản lý dữ liệu HDFS | 1,5–2 giờ | B1–B6 |
| 3 | Spark Standalone | 2–2,5 giờ | C1–C7 |
| 4 | Spark on YARN | 2 giờ | D1–D6 |
| 5 | Viết báo cáo | 2–3 giờ | Toàn bộ |

### Buổi 1: Hoàn tất Giai đoạn 1
- Mở cụm, chụp lại **A2** (cuộn tới `Live Nodes: 2` và tab Datanodes) và **A3** (thu nhỏ trình duyệt 75–80%, thêm trang Nodes).
- **BT1.2:** Tạo `/user/student`, upload `movies.csv`, chạy `down` rồi `up`, kiểm tra file còn không. Giải thích vai trò của volume.
- **BT1.3:** Upload cùng một file với replication 2 và 1 (`-D dfs.replication=1`), so sánh bằng `-ls` và `fsck`.
- Đổi tên ảnh theo quy ước `A1_docker_ps.png`, `A2_namenode.png`, ...

### Buổi 2: Quản lý dữ liệu HDFS

| Bài | Nội dung | Lệnh chính | Ảnh |
| :--- | :--- | :--- | :--- |
| **BT2.1** | Cây thư mục, upload, đổi tên, sao chép, xóa | `mkdir -p`, `put`, `ls -R`, `mv`, `cp`, `rm -r` | B1 |
| **BT2.2** | File lớn chia block | `put /ml25m/ratings.csv` (647 MB, khoảng 6 block), `fsck -files -blocks -locations` | B2, B3 |
| **BT2.3** | Đổi replication | `setrep -w 1`, rồi `setrep -w 2`, so sánh `fsck` | B4 |
| **BT2.4** | Chịu lỗi DataNode | `docker stop datanode2`, chờ khoảng 90 giây, xem `dfsadmin -report` và `fsck`, bật lại và quan sát hồi phục | B5, B6 |
| **BT2.5** | Quyền và dung lượng | `chmod`, `chown`, `du -h`, `df -h` | |
| **BT2.6** | Duyệt file bằng web | NameNode UI → Utilities → Browse the file system | B3 |

- **Lưu ý BT2.4:** Cụm chỉ có 2 DataNode và replication là 2, nên khi `datanode2` chết, các block ở trạng thái `under-replicated` và không thể tự nhân bản sang node thứ ba. Chúng chỉ được phục hồi khi `datanode2` chạy lại. Đây là điểm đáng ghi vào báo cáo.

### Buổi 3: Spark Standalone
- **Chuẩn bị:** Tắt nhóm YARN để tiết kiệm RAM (`docker compose --profile yarn stop`), bật History Server (`up -d --profile history`), rồi tạo thư mục log trên HDFS (`hdfs dfs -mkdir -p /spark-logs`). Bước tạo thư mục là bắt buộc, nếu thiếu thì `spark-submit` sẽ báo lỗi.
- **Dữ liệu:** Bài thường dùng `ratings_1m.csv` (25 MB). Chỉ chạy `ratings.csv` (647 MB) ở một bài so sánh hiệu năng, với `--executor-memory 512m`.

| Bài | Nội dung | Ảnh |
| :--- | :--- | :--- |
| **Ví dụ 1** | `pi.py` tự viết (bản pip không có jar SparkPi) | C2 |
| **BT3.1** | Chạy `pi.py` với 10, 100, 1000 partition, so sánh thời gian | |
| **Ví dụ 2 + BT3.2** | WordCount trên `movies.csv`, kiểm tra output trên HDFS, xem DAG tại cổng 4040 khi job đang chạy | C3, C4 |
| **BT3.3** | Top 10 phim có điểm trung bình cao nhất (ít nhất 50 lượt đánh giá): join `ratings` với `movies`, ghi Parquet | C5 |
| **BT3.4** | So sánh client mode và cluster mode (xem lưu ý bên dưới) | C7 |
| **BT3.5** | Giới hạn tài nguyên bằng `--executor-memory`, `--total-executor-cores`, xem trên Master UI | C1 |
| **BT3.6** | Dừng một worker khi job đang chạy | |
| **BT3.7** | Xem lại job đã chạy xong trên History Server (cổng 18080) | C6 |

- **Điều chỉnh BT3.4:** Spark Standalone không hỗ trợ cluster mode cho ứng dụng Python, mà ta chỉ viết PySpark. Cách xử lý:
  - Ở Standalone, chạy client mode bình thường, rồi thử cluster mode và chụp thông báo lỗi làm ảnh **C7**. Đây là điểm so sánh có giá trị.
  - So sánh client với cluster mode đầy đủ sẽ làm ở Buổi 4 trên YARN, nơi cluster mode hỗ trợ Python.

### Buổi 4: Spark on YARN
- **Chuẩn bị:**
  - Dừng nhóm Standalone, giữ nhóm YARN và History.
  - Upload jar Spark lên HDFS một lần (`/spark-jars`) và khai báo `spark.yarn.jars`. Nếu bỏ qua, mỗi lần submit sẽ phải đẩy khoảng 345 MB lên HDFS, rất chậm.

| Bài | Nội dung | Ảnh |
| :--- | :--- | :--- |
| **BT4.1** | WordCount, client mode, xem trên ResourceManager UI | D1 |
| **BT4.2** | Job MovieLens, cluster mode, lấy log bằng `yarn logs -applicationId` | D4, D5 |
| **BT4.3** | Thay đổi `--num-executors` (1, 2, 4), quan sát container | D3 |
| **BT4.4** | Cố tình đặt `--executor-memory` vượt giới hạn (2048 MB mỗi node), đọc lỗi và sửa | |
| **BT4.5** | Chạy hai job cùng lúc, thấy một job ở trạng thái `ACCEPTED` | D6 |
| **BT4.6** | So sánh client và cluster mode trên YARN, rồi lập bảng so sánh Standalone với YARN | D2 |

- **Giới hạn cần biết:** Mỗi NodeManager chỉ có 2048 MB, nên tổng là 4096 MB. Mỗi container tính khoảng 1024 MB (512 MB cộng overhead), vậy chỉ chạy được khoảng 3–4 container cùng lúc. BT4.3 với 4 executor sẽ có executor phải chờ, nên đó là hiện tượng cần quan sát, không phải lỗi.

### Buổi 5: Báo cáo
- Tổng hợp ảnh theo mã A0–D6, loại ảnh trùng hoặc mờ.
- Viết theo cấu trúc 8 phần đã đề ra: giới thiệu và sơ đồ kiến trúc, từng giai đoạn gồm lý thuyết ngắn, ví dụ, bài tập và ảnh, bảng lỗi đã gặp, kết luận, phụ lục mã nguồn.
- Mục "Khó khăn và cách khắc phục" có sẵn nhiều tư liệu thật từ quá trình làm: tải Spark chậm 38 ngày và cách dùng lại bản pip, lỗi permission denied của Docker và nhóm `docker`, hộp thoại WSL integration, RAM 6 GB nâng lên 10 GB.
- Có thể đóng gói thành file Word hoặc PDF nếu cần.

---

## Những điểm khác so với kế hoạch ban đầu

| Nội dung | Kế hoạch đầu | Kế hoạch mới | Lý do |
| :--- | :--- | :--- | :--- |
| **Ví dụ SparkPi** | Jar có sẵn | `pi.py` tự viết | Bản pip không có jar examples |
| **Dữ liệu WordCount** | `sample.txt` | `movies.csv` | Đã có sẵn, không cần tải thêm |
| **Dữ liệu block lớn** | Tạo bằng `dd` | `ratings.csv` thật 647 MB | Có sẵn trong `ml-25m` |
| **Cluster mode trên Standalone** | So sánh đầy đủ | Chỉ chụp lỗi, so sánh thật trên YARN | Standalone không hỗ trợ cluster mode cho Python |
| **Chạy cụm** | Bật hết | Bật theo từng nhóm (profile) | RAM 10 GB |
| **Thời gian** | 14 ngày | 5 buổi | Theo tiến độ thực tế |

---

## Tiến độ
- [x] Môi trường: WSL, Docker, RAM, quyền docker
- [x] Image build, cụm 10 container chạy, HDFS 2 DataNode, YARN 2 NodeManager, Spark 2 Worker
- [x] Ảnh A1, A4. [x] Chụp lại A2 (thấy Live Nodes = 2), A3 (không bị cắt bảng)
- [x] Buổi 1: BT1.2 (down/up, dữ liệu còn không), BT1.3 (replication 1 và 2)
- [x] Buổi 2: HDFS (BT2.1–2.6, ảnh B1–B6)
- [x] Buổi 3: Spark Standalone (BT3.1–3.7, ảnh C1–C7)
- [x] Buổi 4: Spark on YARN (BT4.1–4.6, ảnh D1–D6)
- [x] Buổi 5: Báo cáo (BAO_CAO_DO_AN.md)
