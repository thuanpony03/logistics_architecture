-- Drop all existing objects
DROP TABLE IF EXISTS db_streaming_logistics.notifications_mv;
DROP TABLE IF EXISTS db_streaming_logistics.kafka_notifications;
DROP TABLE IF EXISTS db_streaming_logistics.notifications;

DROP TABLE IF EXISTS db_streaming_logistics.payments_mv;
DROP TABLE IF EXISTS db_streaming_logistics.kafka_payments;
DROP TABLE IF EXISTS db_streaming_logistics.payments;

DROP TABLE IF EXISTS db_streaming_logistics.shipments_mv;
DROP TABLE IF EXISTS db_streaming_logistics.kafka_shipments;
DROP TABLE IF EXISTS db_streaming_logistics.shipments;

DROP TABLE IF EXISTS db_streaming_logistics.orders_mv;
DROP TABLE IF EXISTS db_streaming_logistics.kafka_orders;
DROP TABLE IF EXISTS db_streaming_logistics.orders;

DROP TABLE IF EXISTS db_streaming_logistics.drivers_mv;
DROP TABLE IF EXISTS db_streaming_logistics.kafka_drivers;
DROP TABLE IF EXISTS db_streaming_logistics.drivers;

DROP TABLE IF EXISTS db_streaming_logistics.users_mv;
DROP TABLE IF EXISTS db_streaming_logistics.kafka_users;
DROP TABLE IF EXISTS db_streaming_logistics.users;

-- Create Users tables
CREATE TABLE db_streaming_logistics.users
(
    user_id UInt32,
    full_name String,
    email String,
    password_hash String,
    phone_number String,
    address String,
    role Enum('user' = 1, 'driver' = 2),
    created_at DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = MergeTree()
ORDER BY (user_id);

CREATE TABLE db_streaming_logistics.kafka_users
(
    user_id UInt32,
    full_name String,
    email String,
    password_hash String,
    phone_number String,
    address String,
    role String,
    created_at DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'logistics.db_logistics.Users',
         kafka_group_name = 'clickhouse_users_consumer',
         kafka_format = 'JSONEachRow',
         date_time_input_format = 'best_effort';

-- Create Drivers tables
CREATE TABLE db_streaming_logistics.drivers
(
    driver_id UInt32,
    user_id UInt32,
    vehicle_license_plate String,
    vehicle_type String,
    vehicle_year UInt16,
    created_at DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = MergeTree()
ORDER BY (driver_id);

CREATE TABLE db_streaming_logistics.kafka_drivers
(
    driver_id UInt32,
    user_id UInt32,
    vehicle_license_plate String,
    vehicle_type String,
    vehicle_year UInt16,
    created_at DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'logistics.db_logistics.Drivers',
         kafka_group_name = 'clickhouse_drivers_consumer',
         kafka_format = 'JSONEachRow',
         date_time_input_format = 'best_effort';

-- Create Orders tables
CREATE TABLE db_streaming_logistics.orders
(
    order_id UInt32,
    user_id UInt32,
    pickup_address String,
    delivery_address String,
    package_description String,
    package_weight Float32,
    delivery_time DateTime64(3),
    status Enum('processing' = 1, 'accepted' = 2, 'in_transit' = 3, 'delivered' = 4),
    created_at DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = MergeTree()
ORDER BY (order_id);

CREATE TABLE db_streaming_logistics.kafka_orders
(
    order_id UInt32,
    user_id UInt32,
    pickup_address String,
    delivery_address String,
    package_description String,
    package_weight Float32,
    delivery_time DateTime64(3),
    status String,
    created_at DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'logistics.db_logistics.Orders',
         kafka_group_name = 'clickhouse_orders_consumer',
         kafka_format = 'JSONEachRow',
         date_time_input_format = 'best_effort';

-- Create Shipments tables
CREATE TABLE db_streaming_logistics.shipments
(
    shipment_id UInt32,
    order_id UInt32,
    driver_id UInt32,
    current_location String,
    estimated_delivery_time DateTime64(3),
    status Enum('assigned' = 1, 'in_transit' = 2, 'completed' = 3),
    created_at DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = MergeTree()
ORDER BY (shipment_id);

CREATE TABLE db_streaming_logistics.kafka_shipments
(
    shipment_id UInt32,
    order_id UInt32,
    driver_id UInt32,
    current_location String,
    estimated_delivery_time DateTime64(3),
    status String,
    created_at DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'logistics.db_logistics.Shipments',
         kafka_group_name = 'clickhouse_shipments_consumer',
         kafka_format = 'JSONEachRow',
         date_time_input_format = 'best_effort';

-- Create Payments tables
CREATE TABLE db_streaming_logistics.payments
(
    payment_id UInt32,
    order_id UInt32,
    amount Decimal64(2),
    payment_method Enum('credit_card' = 1, 'e_wallet' = 2, 'bank_transfer' = 3),
    payment_status Enum('pending' = 1, 'completed' = 2, 'failed' = 3),
    payment_date DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = MergeTree()
ORDER BY (payment_id);

CREATE TABLE db_streaming_logistics.kafka_payments
(
    payment_id UInt32,
    order_id UInt32,
    amount Decimal64(2),
    payment_method String,
    payment_status String,
    payment_date DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'logistics.db_logistics.Payments',
         kafka_group_name = 'clickhouse_payments_consumer',
         kafka_format = 'JSONEachRow',
         date_time_input_format = 'best_effort';

-- Create Notifications tables
CREATE TABLE db_streaming_logistics.notifications
(
    notification_id UInt32,
    user_id UInt32,
    message String,
    notification_date DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = MergeTree()
ORDER BY (notification_id);

CREATE TABLE db_streaming_logistics.kafka_notifications
(
    notification_id UInt32,
    user_id UInt32,
    message String,
    notification_date DateTime64(3),
    _operation String,
    _timestamp Nullable(DateTime)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'logistics.db_logistics.Notifications',
         kafka_group_name = 'clickhouse_notifications_consumer',
         kafka_format = 'JSONEachRow',
         date_time_input_format = 'best_effort';

-- Create Materialized Views
CREATE MATERIALIZED VIEW db_streaming_logistics.users_mv TO db_streaming_logistics.users AS
SELECT
    user_id,
    full_name,
    email,
    password_hash,
    phone_number,
    address,
    multiIf(role = 'user', 'user', role = 'driver', 'driver', 'user') AS role,
    created_at,
    _operation,
    _timestamp
FROM db_streaming_logistics.kafka_users;

CREATE MATERIALIZED VIEW db_streaming_logistics.drivers_mv TO db_streaming_logistics.drivers AS
SELECT * FROM db_streaming_logistics.kafka_drivers;

CREATE MATERIALIZED VIEW db_streaming_logistics.orders_mv TO db_streaming_logistics.orders AS
SELECT
    order_id,
    user_id,
    pickup_address,
    delivery_address,
    package_description,
    package_weight,
    delivery_time,
    multiIf(
        status = 'processing', 'processing',
        status = 'accepted', 'accepted',
        status = 'in_transit', 'in_transit',
        status = 'delivered', 'delivered',
        'processing'
    ) AS status,
    created_at,
    _operation,
    _timestamp
FROM db_streaming_logistics.kafka_orders;

CREATE MATERIALIZED VIEW db_streaming_logistics.shipments_mv TO db_streaming_logistics.shipments AS
SELECT
    shipment_id,
    order_id,
    driver_id,
    current_location,
    estimated_delivery_time,
    multiIf(
        status = 'assigned', 'assigned',
        status = 'in_transit', 'in_transit',
        status = 'completed', 'completed',
        'assigned'
    ) AS status,
    created_at,
    _operation,
    _timestamp
FROM db_streaming_logistics.kafka_shipments;

CREATE MATERIALIZED VIEW db_streaming_logistics.payments_mv TO db_streaming_logistics.payments AS
SELECT
    payment_id,
    order_id,
    amount,
    multiIf(
        payment_method = 'credit_card', 'credit_card',
        payment_method = 'e_wallet', 'e_wallet',
        payment_method = 'bank_transfer', 'bank_transfer',
        'credit_card'
    ) AS payment_method,
    multiIf(
        payment_status = 'pending', 'pending',
        payment_status = 'completed', 'completed',
        payment_status = 'failed', 'failed',
        'pending'
    ) AS payment_status,
    payment_date,
    _operation,
    _timestamp
FROM db_streaming_logistics.kafka_payments;

CREATE MATERIALIZED VIEW db_streaming_logistics.notifications_mv TO db_streaming_logistics.notifications AS
SELECT * FROM db_streaming_logistics.kafka_notifications;