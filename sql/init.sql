-- Initialize PostgreSQL database for Streaming ETL
-- This script creates the necessary tables and views

-- Create raw orders table (mirrors Delta Lake structure)
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_date TIMESTAMP,
    status VARCHAR(20),
    region VARCHAR(50),
    customer_name VARCHAR(100),
    email VARCHAR(100),
    segment VARCHAR(20),
    is_valid BOOLEAN,
    risk_level VARCHAR(10),
    processing_time TIMESTAMP,
    kafka_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    segment VARCHAR(20),
    registration_date TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create order aggregations table
CREATE TABLE IF NOT EXISTS order_aggregations (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    region VARCHAR(50),
    segment VARCHAR(20),
    order_count INTEGER,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    unique_customers INTEGER,
    calculated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create invalid orders table for data quality tracking
CREATE TABLE IF NOT EXISTS invalid_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_date TIMESTAMP,
    status VARCHAR(20),
    region VARCHAR(50),
    customer_name VARCHAR(100),
    email VARCHAR(100),
    segment VARCHAR(20),
    is_valid BOOLEAN,
    risk_level VARCHAR(10),
    processing_time TIMESTAMP,
    kafka_timestamp TIMESTAMP,
    validation_errors TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_raw_orders_order_date ON raw_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_raw_orders_customer_id ON raw_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_region ON raw_orders(region);
CREATE INDEX IF NOT EXISTS idx_raw_orders_segment ON raw_orders(segment);
CREATE INDEX IF NOT EXISTS idx_raw_orders_is_valid ON raw_orders(is_valid);

CREATE INDEX IF NOT EXISTS idx_order_aggregations_window ON order_aggregations(window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_order_aggregations_region ON order_aggregations(region);

-- Create views for common queries
CREATE OR REPLACE VIEW v_hourly_metrics AS
SELECT 
    DATE_TRUNC('hour', order_date) as hour,
    region,
    segment,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(CASE WHEN is_valid = false THEN 1 END) as invalid_orders,
    ROUND(
        COUNT(CASE WHEN is_valid = true THEN 1 END) * 100.0 / COUNT(*), 2
    ) as quality_score
FROM raw_orders
WHERE order_date >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', order_date), region, segment
ORDER BY hour DESC;

CREATE OR REPLACE VIEW v_real_time_summary AS
SELECT 
    COUNT(*) as total_orders,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(CASE WHEN is_valid = false THEN 1 END) as invalid_orders,
    ROUND(
        COUNT(CASE WHEN is_valid = true THEN 1 END) * 100.0 / COUNT(*), 2
    ) as quality_score,
    AVG(EXTRACT(EPOCH FROM (processing_time - order_date))) as avg_latency_seconds,
    MAX(EXTRACT(EPOCH FROM (processing_time - order_date))) as max_latency_seconds
FROM raw_orders 
WHERE order_date >= CURRENT_TIMESTAMP - INTERVAL '1 hour';

-- Create stored procedures for data quality reporting
CREATE OR REPLACE FUNCTION get_data_quality_report(hours_back INTEGER DEFAULT 1)
RETURNS TABLE (
    metric_name VARCHAR,
    metric_value DECIMAL,
    threshold_value DECIMAL,
    status VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    WITH quality_metrics AS (
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN is_valid = true THEN 1 END) as valid_records,
            COUNT(CASE WHEN is_valid = false THEN 1 END) as invalid_records,
            AVG(EXTRACT(EPOCH FROM (processing_time - order_date))) as avg_latency
        FROM raw_orders 
        WHERE order_date >= CURRENT_TIMESTAMP - (hours_back || ' hours')::INTERVAL
    )
    SELECT 
        'Data Quality Score'::VARCHAR,
        ROUND(valid_records * 100.0 / NULLIF(total_records, 0), 2),
        95.0::DECIMAL,
        CASE 
            WHEN ROUND(valid_records * 100.0 / NULLIF(total_records, 0), 2) >= 95 THEN 'PASS'
            ELSE 'FAIL'
        END::VARCHAR
    FROM quality_metrics
    
    UNION ALL
    
    SELECT 
        'Average Latency (seconds)'::VARCHAR,
        ROUND(avg_latency::DECIMAL, 2),
        5.0::DECIMAL,
        CASE 
            WHEN avg_latency <= 5 THEN 'PASS'
            ELSE 'FAIL'
        END::VARCHAR
    FROM quality_metrics
    
    UNION ALL
    
    SELECT 
        'Total Records Processed'::VARCHAR,
        total_records::DECIMAL,
        100.0::DECIMAL,
        CASE 
            WHEN total_records >= 100 THEN 'PASS'
            ELSE 'WARN'
        END::VARCHAR
    FROM quality_metrics;
END;
$$ LANGUAGE plpgsql;

-- Create function to clean old data
CREATE OR REPLACE FUNCTION cleanup_old_data(days_to_keep INTEGER DEFAULT 7)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    -- Delete old raw orders
    DELETE FROM raw_orders 
    WHERE order_date < CURRENT_TIMESTAMP - (days_to_keep || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    -- Delete old aggregations
    DELETE FROM order_aggregations 
    WHERE window_start < CURRENT_TIMESTAMP - (days_to_keep || ' days')::INTERVAL;
    
    -- Delete old invalid orders
    DELETE FROM invalid_orders 
    WHERE order_date < CURRENT_TIMESTAMP - (days_to_keep || ' days')::INTERVAL;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Insert sample data for testing
INSERT INTO customers (customer_id, customer_name, email, segment, registration_date) VALUES
('C0001', 'John Smith', 'john.smith@email.com', 'Premium', '2023-01-15 10:30:00'),
('C0002', 'Jane Doe', 'jane.doe@email.com', 'Standard', '2023-02-20 14:15:00'),
('C0003', 'Bob Johnson', 'bob.johnson@email.com', 'Basic', '2023-03-10 09:45:00'),
('C0004', 'Alice Brown', 'alice.brown@email.com', 'Premium', '2023-01-25 16:20:00'),
('C0005', 'Charlie Wilson', 'charlie.wilson@email.com', 'Standard', '2023-04-05 11:10:00')
ON CONFLICT (customer_id) DO NOTHING;

-- Create notification function for alerts
CREATE OR REPLACE FUNCTION notify_data_quality_issue()
RETURNS TRIGGER AS $$
BEGIN
    -- Send notification when data quality drops below threshold
    IF (
        SELECT COUNT(CASE WHEN is_valid = false THEN 1 END) * 100.0 / COUNT(*)
        FROM raw_orders 
        WHERE order_date >= CURRENT_TIMESTAMP - INTERVAL '10 minutes'
    ) > 10 THEN
        PERFORM pg_notify('data_quality_alert', 
            json_build_object(
                'timestamp', CURRENT_TIMESTAMP,
                'message', 'Data quality below threshold',
                'order_id', NEW.order_id
            )::text
        );
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for data quality monitoring
DROP TRIGGER IF EXISTS trigger_data_quality_check ON raw_orders;
CREATE TRIGGER trigger_data_quality_check
    AFTER INSERT ON raw_orders
    FOR EACH ROW
    EXECUTE FUNCTION notify_data_quality_issue();

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO etl_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO etl_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO etl_user;