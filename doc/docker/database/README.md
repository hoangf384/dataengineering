# Database Layer

Thư mục này dùng để triển khai tầng **Database** cho hệ thống Data Platform, bao gồm:

* Cụm Cassandra 3 node (1 seed + 2 worker node)
* MySQL (OLTP / staging layer)

Tất cả được triển khai bằng Docker Compose.

---

# Kiến trúc tổng thể

## Cassandra Cluster (3 Nodes)

```
cassandra-seed     (Seed Node)
cassandra-node-2
cassandra-node-3
```

* Cluster Name: `LearningCluster`
* Mỗi node giới hạn RAM 2GB
* Dùng `GossipingPropertyFileSnitch`
* 16 tokens mỗi node
* Replication giữa các node trong cùng DC

Port public:

```
9042 → Cassandra CQL client
```

---

## MySQL

* Image: `mysql:lts`
* Port: `3306`
* Dùng cho:

  * Lưu dữ liệu giao dịch
  * Staging trước khi ETL
  * Spark ingest

---

# 1. Cassandra Cluster

## 1.1 Seed Node

Seed node là node đầu tiên của cluster. Các node khác sẽ join thông qua node này.

Cấu hình chính:

* `CASSANDRA_CLUSTER_NAME=LearningCluster`
* `CASSANDRA_SEEDS=cassandra-seed`
* `CASSANDRA_NUM_TOKENS=16`
* Heap:

  * `MAX_HEAP_SIZE=1024M`
  * `HEAP_NEWSIZE=256M`

Volume:

```
cassandra_seed_data:/var/lib/cassandra
```

Healthcheck:

```
cqlsh -e "describe keyspaces"
```

---

## 1.2 Node 2 & Node 3

Hai node này:

* Join cluster thông qua `cassandra-seed`
* Chỉ start khi node trước đã healthy
* Giới hạn RAM 2GB
* Cấu hình heap tương tự seed node

Volumes:

```
cassandra_node2_data:/var/lib/cassandra
cassandra_node3_data:/var/lib/cassandra
```

---

## 1.3 Biến môi trường quan trọng

Các biến lấy từ `.env`:

* `CASSANDRA_DC`
* `CASSANDRA_RACK`

Cluster sử dụng:

```
CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
```

Điều này giúp Cassandra hiểu topology (DC / Rack).

---

# 2. MySQL Configuration

## 2.1 Environment Variables

Lấy từ `.env`:

* MYSQL_ROOT_PASSWORD
* MYSQL_DATABASE
* MYSQL_USER
* MYSQL_PASSWORD
* TZ

---

## 2.2 Custom MySQL Config

Khi start container, MySQL được cấu hình:

```
--character-set-server=utf8mb4
--collation-server=utf8mb4_unicode_ci
--max_connections=200
--innodb_buffer_pool_size=512M
```

Điều này giúp:

* Hỗ trợ full UTF-8
* Tăng số lượng connection
* Tối ưu buffer pool cho workload vừa phải

---

## 2.3 Volume Mount

| Host Path                   | Container Path              | Mục đích           |
| --------------------------- | --------------------------- | ------------------ |
| db-volume                   | /var/lib/mysql              | Lưu data           |
| ~/dataengineering/db/init   | /docker-entrypoint-initdb.d | Script khởi tạo DB |
| ~/dataengineering/db/conf.d | /etc/mysql/conf.d           | Custom config      |

---

# 3. Healthcheck

## Cassandra

```
cqlsh -e "describe keyspaces"
```

## MySQL

```
mysqladmin ping -h localhost
```

Healthcheck giúp:

* Đảm bảo service chỉ được dùng khi đã sẵn sàng
* Phối hợp tốt với depends_on

---

# 4. Resource Limits

| Service              | Memory Limit |
| -------------------- | ------------ |
| Cassandra (mỗi node) | 2G           |
| MySQL                | 1G           |

---

# 5. Cách chạy

Start toàn bộ database:

```bash
docker compose up -d
```

Kiểm tra container:

```bash
docker ps
```

Kiểm tra cluster Cassandra đã vào ring chưa?

```bash
docker exec -it cassandra-seed nodetool status
```

Truy cập MySQL:

```bash
docker exec -it mysql mysql -u root -p
```

---
