# Dự Án Data Engineering: Pipeline ETL với Spark, Airflow, Cassandra và MySQL

Dự án xây dựng hệ thống ETL (Extract, Transform, Load) hoàn chỉnh để xử lý dữ liệu tracking events từ Cassandra, kết hợp với dữ liệu chiều (dimension data) từ MySQL, và tổng hợp metrics trở lại MySQL.

## Mục Đích Dự Án

Dự án cá nhân nhằm hiểu rõ về luồng xử lý dữ liệu (data workflow) trong môi trường phân tán với:
- Xử lý dữ liệu batch sử dụng Apache Spark
- Điều phối workflow tự động với Apache Airflow
- Lưu trữ dữ liệu phân tán với Cassandra
- Quản lý dữ liệu quan hệ với MySQL
- Containerization với Docker

## Kiến Trúc Tổng Quan
![Architecture Overview](images/final-uml-flow-chart.drawio.png)
### Các Thành Phần Chính

**MySQL (Port 3306)**
- Lưu trữ dữ liệu chiều: jobs, campaigns, companies, publishers
- Lưu trữ kết quả cuối cùng sau khi tổng hợp (bảng `events`)
- Lưu trữ metadata watermark để theo dõi tiến độ ETL

**Cassandra Cluster (Port 9042)**
- 3 nodes: cassandra-seed, cassandra-node-2, cassandra-node-3
- Lưu trữ raw tracking events với khả năng mở rộng cao
- Bảng `tracking` với partition key là `event_date` và clustering key là `create_time`

**Apache Spark (Ports 8080-8081)**
- 3 services: master, worker, history-server
- Engine xử lý ETL batch
- Đọc incremental data từ Cassandra, transform và ghi vào MySQL

**Apache Airflow (Port 8080)**
- Điều phối và lập lịch các ETL jobs
- 2 DAGs chính:
  - `gen-dummy-data`: Tạo dữ liệu test
  - `ETL-jobs`: Chạy pipeline ETL chính

### Luồng Dữ Liệu

```
[Tracking Events] → [Cassandra] → [Spark ETL] → [MySQL Events Table]
                                        ↓
                                   [Join with]
                                        ↓
                              [MySQL Dimension Tables]
```

## Cấu Trúc Thư Mục

```
dataengineering/
├── code/
│   ├── extended/                 # Module ETL chính
│   │   ├── config/              # Cấu hình Spark và kết nối
│   │   │   ├── settings.py      # Biến môi trường và constants
│   │   │   └── spark.py         # Factory để tạo SparkSession
│   │   ├── data_io/             # Đọc/ghi dữ liệu
│   │   │   ├── readers.py       # Đọc từ Cassandra và MySQL
│   │   │   ├── writers.py       # Ghi vào MySQL
│   │   │   └── metadata.py      # Quản lý watermark
│   │   ├── process/             # Xử lý và chuyển đổi dữ liệu
│   │   │   ├── transformations.py  # Logic transform chính
│   │   │   └── validations.py   # Validation và data quality
│   │   └── main.py              # Entry point của ETL job
│   └── test/
│       └── dummy-gennerator.py  # Tạo dữ liệu test
├── dags/
│   ├── etl.py                   # DAG ETL chính
│   └── gen-data.py              # DAG tạo dữ liệu test
├── docker/
│   ├── spark/                   # Docker configs cho Spark
│   │   ├── Dockerfile
│   │   ├── docker-compose.yaml
│   │   └── spark-defaults.conf
│   ├── airflow/                 # Docker configs cho Airflow
│   │   ├── Dockerfile
│   │   └── docker-compose.yaml
│   └── database/                # Docker configs cho MySQL & Cassandra
│       └── docker-compose.yaml
├── queries/
│   ├── mysql/
│   │   ├── DDL/                 # Schema definitions
│   │   ├── createdUserMySQL.sql
│   │   └── addcolumnMySQL.sql
│   └── cassandra/
│       └── SchemaDefinedCassandra.sql
├── data/                        # Dữ liệu CSV mẫu
│   ├── mysql/MySQL/
│   │   ├── events.csv
│   │   └── master_publisher.csv
│   └── cassandra/Cassandra/
│       └── tracking_with_event_date.csv
├── .env.example                 # Template biến môi trường
├── requirements.txt             # Python dependencies
└── README.md
```

## Chi Tiết Luồng ETL

### 1. Đọc Dữ Liệu (Extract)

**Từ Cassandra:**
- Đọc incremental từ bảng `tracking` dựa trên watermark timestamp
- Filter theo điều kiện `ts > start_watermark`
- Columns quan trọng: `event_date`, `create_time`, `job_id`, `custom_track`, `bid`, `campaign_id`, `group_id`, `publisher_id`

**Từ MySQL:**
- Đọc dimension data từ bảng `job`
- Lấy thông tin: `job_id`, `company_id`, `campaign_id`, `group_id`

### 2. Xử Lý Dữ Liệu (Transform)

**Normalize Event Time:**
- Chuyển đổi TimeUUID (`create_time`) sang timestamp chuẩn
- Sử dụng UDF custom để parse UUID version 1

**Data Quality Checks:**
- Validate timestamp không null
- Filter custom_track theo các giá trị hợp lệ: `click`, `conversion`, `qualified`, `unqualified`
- Loại bỏ records không hợp lệ

**Aggregation:**
- Group by: `dates`, `hours`, `job_id`, `publisher_id`, `campaign_id`, `group_id`
- Metrics tính toán:
  - `clicks`: Tổng số clicks
  - `bid_set`: Giá bid trung bình
  - `spend_hour`: Tổng chi phí (bid × số clicks)
  - `conversion`: Số lượng conversions
  - `qualified_application`: Ứng dụng đạt yêu cầu
  - `disqualified_application`: Ứng dụng không đạt yêu cầu

**Enrichment:**
- Join với job dimension để lấy thêm `company_id`
- Thêm metadata: `processed_at`, `sources`

### 3. Ghi Dữ Liệu (Load)

- Ghi kết quả vào bảng `events` trong MySQL
- Mode: `append`
- Batch size: 10000 records
- Isolation level: `READ_COMMITTED`
- Cập nhật watermark metadata sau khi ghi thành công

## Cài Đặt và Triển Khai

### Yêu Cầu Hệ Thống

- Docker và Docker Compose
- Python 3.8+
- JDK 11 (cho Spark)
- RAM tối thiểu: 16GB (do chạy nhiều services)
- Disk: 20GB khả dụng

### Biến Môi Trường

Tạo file `.env` từ `.env.example`:

```bash
# Database Configuration
DATABASE_IP=database-host
MYSQL_USER=your_user
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=data_engineering
MYSQL_ROOT_PASSWORD=root_password

# Cassandra Configuration
CASSANDRA_KEYSPACE=recruitment
CASSANDRA_CLUSTER_NAME=your_cluster
CASSANDRA_DC=datacenter1
CASSANDRA_RACK=rack1

# Airflow Configuration
AIRFLOW_UID=1000
AIRFLOW_IP=airflow-scheduler

# Timezone
TZ=Asia/Ho_Chi_Minh
```

### Khởi Động Hệ Thống

**Bước 1: Khởi động Database Layer**

```bash
cd docker/database
docker-compose up -d
```

Chờ Cassandra cluster khởi động hoàn tất (khoảng 2-3 phút):

```bash
docker exec -it cassandra-seed nodetool status
```

**Bước 2: Tạo Schema**

MySQL:
```bash
docker exec -i mysql mysql -uroot -p$MYSQL_ROOT_PASSWORD < queries/mysql/createdUserMySQL.sql
```

Cassandra:
```bash
docker exec -i cassandra-seed cqlsh < queries/cassandra/SchemaDefinedCassandra.sql
```

**Bước 3: Khởi động Spark Cluster**

```bash
cd docker/spark
docker build -t spark-extended:3.5.1 .
docker-compose up -d
```

Kiểm tra Spark UI: `http://localhost:8080`

**Bước 4: Khởi động Airflow**

```bash
cd docker/airflow
docker build -t custom-airflow:2.8.1-python3.8 .
docker-compose up -d
```

Truy cập Airflow UI: `http://localhost:8080` (username/password: `airflow`/`airflow`)

### Cài Đặt Python Dependencies (Local Development)

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# hoặc .venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

## Sử Dụng

### Chạy ETL Pipeline qua Airflow

1. Truy cập Airflow Web UI: `http://localhost:8080`
2. Bật DAG `ETL-jobs`
3. Trigger DAG manually hoặc đợi schedule (`@hourly`)
4. Theo dõi logs và task execution

### Tạo Dữ Liệu Test

Trigger DAG `gen-dummy-data` trong Airflow hoặc chạy trực tiếp:

```bash
spark-submit \
  --master local[*] \
  --conf spark.cassandra.connection.host=localhost \
  code/test/dummy-gennerator.py
```

### Chạy ETL Local (Development)

```bash
export DATABASE_IP=localhost
export MYSQL_USER=spark
export MYSQL_PASSWORD=spark
export MYSQL_DATABASE=data_engineering
export CASSANDRA_KEYSPACE=recruitment

spark-submit \
  --master local[*] \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,mysql:mysql-connector-java:8.0.33 \
  code/extended/main.py
```

## Monitoring và Logging

### Spark

- Spark Master UI: `http://localhost:8080`
- Spark Worker UI: `http://localhost:8081`
- Spark History Server: `http://localhost:18080`
- Logs được lưu vào S3 (cấu hình trong spark-defaults.conf)

### Airflow

- Web UI: `http://localhost:8080`
- Logs: `docker/airflow/logs/`
- Remote logs trên S3: `s3://spark-log-proj/airflow-logs`

### Database

MySQL logs:
```bash
docker logs mysql
```

Cassandra logs:
```bash
docker logs cassandra-seed
docker logs cassandra-node-2
docker logs cassandra-node-3
```

## Queries Hữu Ích

### Kiểm tra dữ liệu MySQL

```sql
-- Xem events đã aggregate
SELECT * FROM events ORDER BY dates DESC, hours DESC LIMIT 10;

-- Xem watermark
SELECT * FROM watermark WHERE table_name = 'tracking_etl';

-- Thống kê theo campaign
SELECT campaign_id, SUM(clicks) as total_clicks, SUM(spend_hour) as total_spend
FROM events
GROUP BY campaign_id
ORDER BY total_spend DESC;
```

### Kiểm tra dữ liệu Cassandra

```cql
-- Đếm số records
SELECT COUNT(*) FROM recruitment.tracking;

-- Xem dữ liệu gần nhất
SELECT * FROM recruitment.tracking
WHERE event_date = '2026-01-29'
LIMIT 10;

-- Thống kê theo custom_track
SELECT custom_track, COUNT(*)
FROM recruitment.tracking
WHERE event_date = '2026-01-29'
GROUP BY custom_track;
```

## Xử Lý Sự Cố

### Cassandra không khởi động

```bash
# Kiểm tra logs
docker logs cassandra-seed

# Restart cluster
cd docker/database
docker-compose restart cassandra-seed cassandra-node-2 cassandra-node-3
```

### Spark job fail

```bash
# Xem logs trong Airflow UI hoặc
docker logs airflow-scheduler

# Kiểm tra Spark worker
docker logs spark-worker
```

### Connection timeout

Đảm bảo các services đã khởi động đầy đủ:
```bash
docker ps -a
```

## Tối Ưu Hóa

### Tăng Performance

1. Điều chỉnh Spark configs trong `spark-defaults.conf`:
   - Tăng `spark.executor.memory`
   - Tăng `spark.driver.memory`
   - Điều chỉnh `spark.sql.shuffle.partitions`

2. Cassandra tuning:
   - Tăng `MAX_HEAP_SIZE`
   - Điều chỉnh `CASSANDRA_NUM_TOKENS`

3. MySQL optimization:
   - Index trên `dates`, `hours`, `job_id`
   - Tăng `innodb_buffer_pool_size`

### Scaling

- Thêm Spark workers: Sửa `docker-compose.yaml` trong `docker/spark`
- Scale Cassandra: Thêm nodes trong `docker/database/docker-compose.yaml`
- Tăng Airflow parallelism: Sửa `AIRFLOW__CORE__PARALLELISM`

## Tài Liệu Tham Khảo

- Apache Spark: https://spark.apache.org/docs/latest/
- Apache Airflow: https://airflow.apache.org/docs/
- Apache Cassandra: https://cassandra.apache.org/doc/latest/
- MySQL: https://dev.mysql.com/doc/
- Spark-Cassandra Connector: https://github.com/datastax/spark-cassandra-connector

## Ghi Chú

Đây là dự án học tập và phát triển. Một số cấu hình chưa phù hợp cho production:
- Passwords hardcoded trong `.env`
- Không có authentication cho Spark UI
- Cassandra chạy single datacenter
- Không có backup/disaster recovery plan

Trước khi deploy production, cần review và cải thiện security, monitoring, và reliability.
