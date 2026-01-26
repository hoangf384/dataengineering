CREATE TABLE mydb.events_metadata (
    id INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    max_event_time DATETIME(6) NOT NULL,
    min_event_time DATETIME(6),
    row_count INT DEFAULT 0,
    status VARCHAR(20) DEFAULT 'SUCCESS',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_pipeline_status (pipeline_name, status, max_event_time)
);
