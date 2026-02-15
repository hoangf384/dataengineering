# Spark Cluster (Standalone) 

Thư mục này dùng để triển khai **Apache Spark Standalone Cluster** bằng Docker Compose.

Cluster bao gồm:

* Spark Master
* Spark Worker
* Spark History Server
* Lưu event log lên S3 (`s3a://`)
* Hỗ trợ kết nối MySQL
* Hỗ trợ kết nối Cassandra

---

# Mục lục

1. [Tổng quan](#tổng-quan)
2. [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)

   * [Docker Image tuỳ chỉnh](#docker-image-tuỳ-chỉnh)
   * [Truy cập Web UI](#truy-cập-web-ui)
3. [Cấu hình S3](#cấu-hình-s3)

   * [Xác thực AWS](#xác-thực-aws)
   * [Giới hạn tài nguyên](#giới-hạn-tài-nguyên)
   * [Volume Mount](#volume-mount)
4. [Debug và kiểm tra](#debug-và-kiểm-tra)
5. [Lưu ý quan trọng](#lưu-ý-quan-trọng)

---

# Tổng quan

Cluster được triển khai theo mô hình Standalone:

```
Spark Master      → Port 7077 (Cluster)
                  → Port 8080 (Web UI)

Spark Worker      → Port 8081 (Web UI)

Spark History     → Port 18080 (Web UI)
                  → Đọc event log từ S3
```

Event log được lưu tại:

```
s3a://spark-log-proj/spark-events/
```

---

# Kiến trúc hệ thống

## Docker Image tuỳ chỉnh

Base image sử dụng:

```
apache/spark:3.5.1-scala2.12-java11-python3-ubuntu
```

Trong Dockerfile có bổ sung các thư viện sau:

| Thư viện                  | Mục đích                  |
| ------------------------- | ------------------------- |
| hadoop-aws                | Cho phép Spark đọc/ghi S3 |
| aws-java-sdk-bundle       | AWS SDK                   |
| mysql-connector           | Kết nối MySQL             |
| spark-cassandra-connector | Kết nối Cassandra         |

Các file `.jar` được tải từ Maven Central trong quá trình build image.

---

## Truy cập Web UI

| Service       | URL                                              |
| ------------- | ------------------------------------------------ |
| Spark Master  | [http://localhost:8080](http://localhost:8080)   |
| Spark Worker  | [http://localhost:8081](http://localhost:8081)   |
| Spark History | [http://localhost:18080](http://localhost:18080) |

---

# Cấu hình S3

Spark được cấu hình sử dụng `s3a://` để:

* Lưu event log
* Cho History Server đọc lại các job đã chạy

Cấu hình trong `spark-defaults.conf`:

```properties
spark.eventLog.enabled           true
spark.eventLog.dir               s3a://spark-log-proj/spark-events/
spark.history.fs.logDirectory    s3a://spark-log-proj/spark-events/
```

---

## Xác thực AWS

Cluster sử dụng:

```
InstanceProfileCredentialsProvider
```

Điều này có nghĩa:

* Không lưu access key trong container
* EC2 phải được gán IAM Role có quyền truy cập S3

Ví dụ quyền cần thiết:

* s3:GetObject
* s3:PutObject
* s3:ListBucket

---

## Giới hạn tài nguyên

| Service              | CPU | RAM  |
| -------------------- | --- | ---- |
| spark-master         | 0.5 | 1G   |
| spark-worker         | 1.5 | 5.5G |
| spark-history-server | 0.5 | 1G   |

Cấu hình Worker:

```
SPARK_WORKER_CORES=1
SPARK_WORKER_MEMORY=4g
```

---

## Volume Mount

| Host                   | Container         | Mục đích         |
| ---------------------- | ----------------- | ---------------- |
| ~/dataengineering/data | /data             | Lưu dữ liệu      |
| ~/dataengineering/code | /code             | Chứa Spark job   |
| ./spark-defaults.conf  | Spark config      | Cấu hình cluster |

---

# Debug và kiểm tra

Xem container đang chạy:

```bash
docker ps
```

Xem log:

```bash
docker logs -f spark-master
docker logs -f spark-worker
docker logs -f spark-history-server
```

Truy cập vào container:

```bash
docker exec -it spark-master bash
```

---

# Lưu ý quan trọng

* Bucket S3 phải tồn tại trước khi khởi động cluster.
* IAM Role phải có quyền truy cập S3.
* Không nên hardcode AWS credentials trong Docker.
* History Server được cấu hình dọn log sau 7 ngày.

---

Nếu bạn muốn mình chỉnh thêm theo hướng “portfolio chuẩn platform engineer” (viết thêm phần luồng dữ liệu Airflow → Spark → S3 → Superset) mình có thể bổ sung tiếp cho bạn.
