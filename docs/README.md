# Tài liệu dự án

Tổng hợp tài liệu kỹ thuật cho hệ thống **Job Board Analytics Pipeline** (Cassandra → Spark → MySQL).

---

## Mục lục

### Infrastructure

| Tài liệu | Mô tả |
|---|---|
| [Airflow Setup](./docker/airflow/README.md) | Cấu hình Airflow Docker (image tùy chỉnh, postgres backend, scheduler) |
| [Database Setup](./docker/database/README.md) | Cassandra cluster 3 node + MySQL trên Docker |
| [Spark Setup](./docker/spark/README.md) | Spark Standalone cluster, tích hợp S3 / MySQL / Cassandra |
| [EC2 Setup](./aws/ec2/README.md) | Tạo EC2, cài Docker, cấu hình Security Group & IAM Role |

### Development

| Tài liệu | Mô tả |
|---|---|
| [Kiến trúc code ETL](./code/README.md) | Giải thích chi tiết toàn bộ pipeline: `based`, `extended`, `test` |
| [Kết nối Database](./connectingDB.md) | Hướng dẫn kết nối Spark → MySQL / Cassandra qua Tailscale |

---

## Luồng dữ liệu tổng quan

```
Cassandra (tracking data)
        ↓
    Spark ETL
        ↓
  MySQL (events table)
        ↓
    Airflow (orchestration)
        ↓
    S3 (logs & storage)
```
