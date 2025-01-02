-- Create the Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,       -- e.g., 20241226
    full_date DATE NOT NULL,        -- The actual date
    day_of_week VARCHAR(20),
    day_of_month INT,
    month INT,
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
) ENGINE=InnoDB;

-- Create the User Dimension
CREATE TABLE IF NOT EXISTS dim_user (
    user_key INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    full_name VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(50),
    address VARCHAR(255),
    role VARCHAR(50),
    created_at TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    effective_start_date DATE,
    effective_end_date DATE
) ENGINE=InnoDB;

-- Create the Driver Dimension
CREATE TABLE IF NOT EXISTS dim_driver (
    driver_key INT AUTO_INCREMENT PRIMARY KEY,
    driver_id INT NOT NULL,
    user_key INT NOT NULL,
    full_name VARCHAR(255),
    vehicle_license_plate VARCHAR(50),
    vehicle_type VARCHAR(50),
    vehicle_year INT,
    is_active BOOLEAN DEFAULT TRUE,
    effective_start_date DATE,
    effective_end_date DATE,
    FOREIGN KEY (user_key) REFERENCES dim_user(user_key)
) ENGINE=InnoDB;

-- Create the Location Dimension
CREATE TABLE IF NOT EXISTS dim_location (
    location_key INT AUTO_INCREMENT PRIMARY KEY,
    full_address VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    district VARCHAR(100),
    effective_start_date DATE,
    effective_end_date DATE
) ENGINE=InnoDB;

---- Create the Payment Method Dimension
--CREATE TABLE IF NOT EXISTS dim_payment_method (
--    payment_method_key INT PRIMARY KEY,
--    payment_method VARCHAR(50) NOT NULL,
--    payment_method_category VARCHAR(100)
--) ENGINE=InnoDB;

-- Create the Orders Fact Table
-- This table references several dimension keys for a proper star schema.
CREATE TABLE IF NOT EXISTS fact_orders (
    order_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    user_key INT NOT NULL,
    order_date_key INT,
    pickup_location_key INT,
    delivery_location_key INT,
    package_weight DECIMAL(10, 2),
    order_amount DECIMAL(10, 2),
    delivery_duration_minutes INT,
    order_status VARCHAR(50),
    payment_status VARCHAR(50),
    order_created_at DATETIME,
    order_completed_at DATETIME,
    etl_timestamp DATETIME,
    etl_user VARCHAR(50),
    FOREIGN KEY (user_key) REFERENCES dim_user(user_key),
    FOREIGN KEY (order_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (pickup_location_key) REFERENCES dim_location(location_key),
    FOREIGN KEY (delivery_location_key) REFERENCES dim_location(location_key)
) ENGINE=InnoDB;

-- Create the Shipment Tracking Fact Table
CREATE TABLE IF NOT EXISTS fact_shipment_tracking (
    tracking_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    shipment_id INT NOT NULL,
    order_key BIGINT,
    driver_key INT,
    date_key INT,
    status VARCHAR(50),
    tracking_timestamp DATETIME,
    current_location VARCHAR(255),
    elapsed_time_minutes INT,
    etl_timestamp DATETIME,
    etl_user VARCHAR(50),
    FOREIGN KEY (order_key) REFERENCES fact_orders(order_key),
    FOREIGN KEY (driver_key) REFERENCES dim_driver(driver_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
) ENGINE=InnoDB;