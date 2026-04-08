CREATE DATABASE IF NOT EXISTS traffic_warning;
USE traffic_warning;

CREATE TABLE IF NOT EXISTS warnings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    segment_id INT NOT NULL,
    grade VARCHAR(20) NOT NULL,
    avg_speed DECIMAL(10,2),
    total_vehicles DECIMAL(10,2),
    window_start BIGINT,
    window_end BIGINT,
    level VARCHAR(20),
    warning_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_segment (segment_id),
    INDEX idx_time (warning_time)
);