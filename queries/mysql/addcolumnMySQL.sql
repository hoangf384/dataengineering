use mydb;
ALTER TABLE events ADD COLUMN processed_at DATETIME;
ALTER TABLE events ADD COLUMN event_watermark DATETIME;