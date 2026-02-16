# Báo cáo kỹ thuật: Data Engineering Project

## Giới thiệu

Đây là tài liệu giải thích kiến trúc và luồng hoạt động của dự án data engineering. Mục tiêu của dự án là xây dựng một hệ thống ETL (Extract, Transform, Load) để xử lý dữ liệu tracking người dùng từ Cassandra, tổng hợp và làm giàu dữ liệu, sau đó ghi kết quả vào MySQL cho mục đích phân tích.

Dự án được chia thành hai phần chính:

1.  **`code/based`**: Chứa các script ETL đơn giản, xử lý toàn bộ dữ liệu (full load).
2.  **`code/extended`**: Chứa phiên bản ETL nâng cao, được module hóa, hỗ trợ xử lý dữ liệu tăng trưởng (incremental load) và có khả năng tái sử dụng cao.
3.  **`code/test`**: Chứa script để sinh dữ liệu giả lập cho việc kiểm thử.

---

## 1. `code/based`: Script ETL cơ bản

Thư mục này chứa các phiên bản đầu tiên của pipeline ETL.

### 1.1. `ETL.py`

Đây là một script ETL hoàn chỉnh trong một file duy nhất, thực hiện quy trình sau:

-   **Khởi tạo Spark Session**: Cấu hình và tạo một Spark Session để làm việc.
-   **Đọc dữ liệu**:
    -   Đọc dữ liệu tracking từ bảng `tracking` trong Cassandra.
    -   Đọc dữ liệu chiều (dimension) về `job` từ MySQL.
-   **Kiểm tra Schema**: Đảm bảo các cột cần thiết có tồn tại trong DataFrame.
-   **Transform (Biến đổi)**:
    -   **`normalize_event_time`**: Chuyển đổi cột `create_time` (kiểu TimeUUID) từ Cassandra thành timestamp (`ts`) có thể đọc được.
    -   **`clean_and_filter_custom_track`**: Lọc và làm sạch cột `custom_track`, loại bỏ các giá trị không hợp lệ.
    -   **`aggregate_metrics_single_pass`**: Thực hiện tổng hợp dữ liệu. Dữ liệu được nhóm theo `dates`, `hours`, `job_id`, `publisher_id`, `campaign_id`, `group_id` và các chỉ số sau được tính toán:
        -   `clicks`: Số lượt click.
        -   `bid_set`: Giá thầu trung bình cho mỗi lượt click.
        -   `spend_hour`: Tổng chi tiêu trong một giờ.
        -   `conversion`: Số lượt chuyển đổi.
        -   `qualified_application`: Số đơn ứng tuyển hợp lệ.
        -   `disqualified_application`: Số đơn ứng tuyển không hợp lệ.
    -   **`enrich_job_dimension`**: Làm giàu dữ liệu đã tổng hợp bằng cách join với dữ liệu `job` từ MySQL.
    -   **`add_metadata`**: Thêm các cột metadata như `processed_at` (thời gian xử lý) và `sources` (nguồn dữ liệu).
-   **Ghi dữ liệu**: Ghi DataFrame cuối cùng vào bảng `events` trong MySQL.

**Luồng hoạt động**: `control_flow` là hàm chính điều phối toàn bộ quy trình từ đọc, biến đổi đến ghi dữ liệu.

### 1.2. `ETL_EXTANED.py`

Đây là phiên bản cải tiến của `ETL.py` với các tính năng bổ sung để xử lý dữ liệu tăng trưởng (incremental):

-   **Watermarking**:
    -   **`get_start_watermark`**: Lấy mốc thời gian (watermark) của lần chạy thành công cuối cùng từ bảng `event_metadata` trong MySQL. Điều này cho phép pipeline chỉ xử lý dữ liệu mới hơn mốc thời gian đó.
    -   **`update_watermark`**: Sau khi xử lý xong một batch, cập nhật lại watermark trong bảng `event_metadata` với mốc thời gian mới nhất.
-   **Đọc dữ liệu tăng trưởng (`read_tracking_incremental`)**:
    -   Sử dụng predicate pushdown để chỉ đọc dữ liệu từ Cassandra có `create_time` lớn hơn watermark.
    -   Hàm `get_min_timeuuid_str` được sử dụng để chuyển đổi watermark (dạng `datetime`) thành chuỗi TimeUUID tương ứng để Cassandra có thể lọc hiệu quả.
-   **Luồng hoạt động**: Tương tự như `ETL.py`, nhưng bắt đầu bằng việc lấy watermark và kết thúc bằng việc cập nhật lại watermark.

---

## 2. `code/extended`: Kiến trúc Module hóa

Thư mục này chứa phiên bản ETL được tái cấu trúc thành các module riêng biệt, giúp dễ dàng quản lý, bảo trì và mở rộng.

### 2.1. `main.py`

Đây là file thực thi chính (entry point) của pipeline.

-   **`control_flow`**:
    1.  **Lấy Watermark**: Gọi `get_start_watermark` để xác định điểm bắt đầu xử lý.
    2.  **Đọc dữ liệu**:
        -   Gọi `read_tracking_incremental` để đọc dữ liệu mới từ Cassandra.
        -   Gọi `read_jobs_dimension` để đọc dữ liệu chiều từ MySQL.
    3.  **Kiểm tra dữ liệu rỗng**: Nếu không có dữ liệu mới, pipeline sẽ kết thúc.
    4.  **Transform**: Gọi hàm `transform_data` để thực hiện tất cả các bước biến đổi.
    5.  **Tính toán Watermark mới**: Xác định min và max timestamp của batch vừa xử lý để cập nhật watermark.
    6.  **Ghi dữ liệu**: Gọi `write_data` để ghi kết quả vào MySQL.
    7.  **Cập nhật Watermark**: Gọi `update_watermark` để ghi lại trạng thái của lần chạy.

### 2.2. `config`: Module cấu hình

-   **`settings.py`**:
    -   Định nghĩa các biến môi trường (kết nối database) và các hằng số (schema, `NUM_100NS_INTERVALS_SINCE_UUID_EPOCH`).
    -   Tập trung tất cả các cấu hình vào một nơi, dễ dàng thay đổi.
-   **`spark.py`**:
    -   Cung cấp hàm `get_spark_session` để tạo và cấu hình Spark Session.
    -   Tách biệt logic khởi tạo Spark ra khỏi pipeline chính.

### 2.3. `data_io`: Module Input/Output

-   **`readers.py`**:
    -   **`read_tracking_incremental`**: Chịu trách nhiệm đọc dữ liệu tracking từ Cassandra một cách tăng trưởng, dựa trên `start_time`.
    -   **`read_jobs_dimension`**: Đọc dữ liệu chiều về `job` từ MySQL.
-   **`writers.py`**:
    -   **`write_data`**: Ghi DataFrame đã xử lý vào một bảng trong MySQL. Có xử lý exception và logging.
-   **`metadata.py`**:
    -   **`get_start_watermark`**: Lấy watermark từ bảng metadata.
    -   **`update_watermark`**: Cập nhật watermark sau mỗi lần chạy.

### 2.4. `process`: Module xử lý dữ liệu

-   **`transformations.py`**:
    -   Chứa tất cả các hàm transform logic:
        -   `timeuuid_to_ts`: UDF chuyển đổi TimeUUID.
        -   `normalize_event_time`: Áp dụng UDF vào DataFrame.
        -   `aggregate_metrics_single_pass`: Logic tổng hợp metrics.
        -   `enrich_job_dimension`: Logic làm giàu dữ liệu.
        -   `add_metadata`: Thêm metadata.
    -   Hàm `transform_data` điều phối các bước transform nhỏ.
-   **`validations.py`**:
    -   Chứa các hàm kiểm tra chất lượng dữ liệu:
        -   **`clean_and_filter_custom_track`**: Lọc các giá trị `custom_track` không hợp lệ.
        -   **`validate_event_timestamp`**: Kiểm tra tỉ lệ timestamp không hợp lệ và dừng pipeline nếu vượt ngưỡng.

---

## 3. `code/test`: Module sinh dữ liệu

### 3.1. `dummy_gennerator.py`

Script này được sử dụng để sinh dữ liệu giả lập cho bảng `tracking` trong Cassandra, phục vụ cho việc kiểm thử (testing) và phát triển.

-   **Logic**:
    1.  **Kết nối Spark**: Khởi tạo Spark Session.
    2.  **Lấy dữ liệu tham chiếu**: Đọc danh sách `job` và `publisher` từ MySQL để đảm bảo dữ liệu sinh ra có tính toàn vẹn tham chiếu.
    3.  **Sinh dữ liệu (`generate_and_write_batch`)**:
        -   Tạo ra một số lượng bản ghi ngẫu nhiên.
        -   Mỗi bản ghi chứa các thông tin như `job_id`, `publisher_id` (lấy ngẫu nhiên từ dữ liệu tham chiếu), `custom_track`, `bid`,...
        -   Sử dụng `uuid.uuid1()` để tạo TimeUUID, từ đó trích xuất ra `create_time`, `ts`, và `event_date`.
    4.  **Ghi vào Cassandra**: Tạo DataFrame từ dữ liệu đã sinh và ghi vào bảng `tracking`.

-   **Cấu trúc mới (sau refactor)**:
    -   **`Config` class**: Quản lý tất cả các biến cấu hình.
    -   **`DataGenerator` class**: Đóng gói tất cả logic sinh dữ liệu, bao gồm đọc dữ liệu tham chiếu, tạo bản ghi, và ghi vào Cassandra. Điều này giúp mã nguồn trở nên có tổ chức và dễ đọc hơn.

---

## Kết luận

Kiến trúc của dự án đã phát triển từ một script đơn giản sang một hệ thống ETL module hóa, linh hoạt. Việc tách nhỏ các thành phần (config, I/O, process) giúp tăng khả năng tái sử dụng, dễ bảo trì và mở rộng trong tương lai. Việc sử dụng watermarking cho phép xử lý dữ liệu hiệu quả hơn bằng cách chỉ nạp dữ liệu mới, giảm tải cho cả hệ thống nguồn và đích.
