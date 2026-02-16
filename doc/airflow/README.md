# Cài đặt Môi trường Airflow

## Mục lục
1. [Tổng quan](#tổng-quan)
2. [Yêu cầu tiên quyết](#yêu-cầu-tiên-quyết)
3. [Cài đặt](#cài-đặt)
4. [Chạy Airflow](#chạy-airflow)
5. [Chi tiết Cấu hình](#chi-tiết-cấu-hình)
    - [Docker Compose (`docker-compose.yaml`)](#docker-compose-docker-composeyaml)
    - [Dockerfile](#dockerfile)

Tài liệu này mô tả việc cài đặt và cấu hình cho môi trường Apache Airflow được định nghĩa trong thư mục `docker/airflow`. Thiết lập này bao gồm một image Airflow tùy chỉnh với Spark và các trình kết nối cơ sở dữ liệu, cùng với một backend PostgreSQL cho siêu dữ liệu của Airflow.

## Tổng quan

Tệp `docker-compose.yaml` điều phối các dịch vụ sau:
- **`postgres`**: Một cơ sở dữ liệu PostgreSQL dùng làm kho lưu trữ siêu dữ liệu của Airflow.
- **`airflow-init`**: Một dịch vụ khởi tạo giúp thiết lập các thư mục Airflow, di chuyển (migrate) cơ sở dữ liệu và tạo người dùng quản trị ban đầu.
- **`airflow-webserver`**: Giao diện người dùng (UI) của Airflow, có thể truy cập qua HTTP.
- **`airflow-scheduler`**: Thành phần chịu trách nhiệm lập lịch cho các DAG.

Tệp `Dockerfile` xây dựng một image Airflow tùy chỉnh (`custom-airflow:2.8.1-python3.8`) dựa trên `apache/airflow:2.8.1-python3.8`. Image tùy chỉnh này bao gồm:
- **Java 11**: Yêu cầu cho Spark.
- **Apache Spark 3.5.1 với Hadoop 3**: Được cài đặt trong `/opt/spark`.
- **Các tệp Jar AWS S3**: `hadoop-aws` và `aws-java-sdk-bundle` để kết nối với S3.
- **Trình kết nối cơ sở dữ liệu**: `mysql-connector-j` cho MySQL và `spark-cassandra-connector-assembly` cho Cassandra.
- **Các gói Python**: `pyspark`, `apache-airflow-providers-apache-spark`, và `apache-airflow-providers-amazon`.

## Yêu cầu tiên quyết

- Đã cài đặt Docker và Docker Compose.
- Tệp `.env` trong thư mục `docker/airflow` (hoặc trong thư mục gốc của dự án nếu được tham chiếu từ đó) với các biến `AIRFLOW_UID` và `TZ` được định nghĩa (ví dụ: `AIRFLOW_UID=50000`, `TZ=Asia/Ho_Chi_Minh`).
- Quyền truy cập vào một S3 bucket có tên `spark-log-proj` để lưu trữ log Airflow từ xa, và một kết nối AWS có tên `aws_default` được cấu hình trong Airflow.

## Cài đặt

1.  **Xây dựng image Airflow tùy chỉnh**:
    Tệp `docker-compose.yaml` được cấu hình để tự động xây dựng image.

2.  **Khởi tạo Airflow**:
    Dịch vụ `airflow-init` sẽ tạo các thư mục cần thiết, di chuyển cơ sở dữ liệu và thiết lập một người dùng quản trị Airflow ban đầu.

## Chạy Airflow

1.  Di chuyển đến thư mục `docker/airflow`.
2.  Khởi động các dịch vụ:
    ```bash
    docker-compose up -d
    ```
3.  Truy cập giao diện người dùng Airflow:
    Mở trình duyệt web của bạn và truy cập `http://localhost:8080`.
    Tên người dùng và mật khẩu mặc định được tạo bởi `airflow-init` đều là `airflow`.

## Chi tiết Cấu hình

### Docker Compose (`docker-compose.yaml`)

-   **Image**: `custom-airflow:2.8.1-python3.8` được xây dựng từ `Dockerfile` cục bộ.
-   **Biến môi trường**: Được tải từ tệp `.env` và được mã hóa cứng trong `docker-compose.yaml`. Các biến môi trường chính bao gồm:
    -   `AIRFLOW__CORE__EXECUTOR`: `LocalExecutor`
    -   `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` và `AIRFLOW__CORE__SQL_ALCHEMY_CONN`: Trỏ đến dịch vụ `postgres`.
    -   `AIRFLOW__CORE__LOAD_EXAMPLES`: `false`
    -   `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`: `true`
    -   `PYTHONPATH`: `/opt/project` (ánh xạ tới `~/dataengineering/code`)
    -   **Ghi log từ xa**: `AIRFLOW__LOGGING__REMOTE_LOGGING=true`, `AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://spark-log-proj/airflow-logs`, `AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_default`.
-   **Volumes (Ánh xạ thư mục)**:
    -   `~/dataengineering/dags:/opt/airflow/dags:z`
    -   `./logs:/opt/airflow/logs:z`
    -   `./config:/opt/airflow/config:z`
    -   `./plugins:/opt/airflow/plugins:z`
    -   `~/dataengineering/code:/opt/project/code:z`
    -   `../spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:z`
-   **Ports (Cổng)**:
    -   Webserver: `8080:8080`
    -   Scheduler: `30000:30000`, `30001:30001` (Các cổng này có thể dành cho giao tiếp nội bộ hoặc các cổng liên quan đến Spark).
-   **Cấu hình riêng cho `airflow-init`**:
    -   `_AIRFLOW_DB_MIGRATE`: `true`
    -   `_AIRFLOW_WWW_USER_CREATE`: `true`
    -   `_AIRFLOW_WWW_USER_USERNAME`: `airflow`
    -   `_AIRFLOW_WWW_USER_PASSWORD`: `airflow`

### Dockerfile

-   **Image cơ sở**: `apache/airflow:2.8.1-python3.8`
-   **Cài đặt Java**: Cài đặt `openjdk-11-jre-headless`.
-   **Cài đặt Spark**: Tải xuống và giải nén Spark 3.5.1 cho Hadoop 3.
-   **Các tệp JAR**:
    -   `hadoop-aws-3.3.4.jar`
    -   `aws-java-sdk-bundle-1.12.262.jar`
    -   `mysql-connector-j-8.4.0.jar`
    -   `spark-cassandra-connector-assembly_2.12-3.5.1.jar`
    Các tệp JAR này được đặt trong `${SPARK_HOME}/jars`.
-   **Các gói phụ thuộc Python**: Cài đặt `pyspark==3.5.1`, `apache-airflow-providers-apache-spark`, và `apache-airflow-providers-amazon`.
