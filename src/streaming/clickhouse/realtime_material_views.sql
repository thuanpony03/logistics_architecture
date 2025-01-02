-- 1. Real-time Order Monitoring Materialized View
DROP TABLE IF EXISTS db_streaming_logistics.mv_realtime_order_status;
CREATE MATERIALIZED VIEW db_streaming_logistics.mv_realtime_order_status
ENGINE = MergeTree()
ORDER BY (order_id, order_time)
POPULATE AS
SELECT DISTINCT
    o.order_id AS order_id,
    o.created_at AS order_time,
    u.full_name AS customer_name,
    o.pickup_address AS pickup_address,
    o.delivery_address AS delivery_address,
    o.status AS order_status,
    s.current_location AS current_location,
    s.estimated_delivery_time AS estimated_delivery_time,
    d.full_name AS driver_name,
    multiIf(
        o.status = 4, 'Completed',
        now() > s.estimated_delivery_time, 'Delayed',
        'On Time'
    ) AS delivery_status,
    p.payment_status AS payment_status
FROM db_streaming_logistics.orders AS o
LEFT JOIN db_streaming_logistics.users AS u ON o.user_id = u.user_id
LEFT JOIN db_streaming_logistics.shipments AS s ON o.order_id = s.order_id
LEFT JOIN db_streaming_logistics.drivers AS dr ON s.driver_id = dr.driver_id
LEFT JOIN db_streaming_logistics.users AS d ON dr.user_id = d.user_id
LEFT JOIN db_streaming_logistics.payments AS p ON o.order_id = p.order_id;

-- 2. Real-time Driver Performance Materialized View
DROP TABLE IF EXISTS db_streaming_logistics.mv_driver_metrics;
CREATE MATERIALIZED VIEW db_streaming_logistics.mv_driver_metrics
ENGINE = AggregatingMergeTree()
ORDER BY (driver_id, created_at)
POPULATE AS
SELECT
    d.driver_id AS driver_id,
    u.full_name AS driver_name,
    s.created_at AS created_at,
    COUNT(DISTINCT s.shipment_id) AS total_deliveries,
    countState(multiIf(s.status = 2, 1, NULL)) AS active_deliveries,
    countState(multiIf(s.status = 3, 1, NULL)) AS completed_deliveries,
    avgState(multiIf(s.status = 3, dateDiff('minute', s.created_at, s.estimated_delivery_time), NULL)) AS avg_delivery_time_mins,
    countState(multiIf((now() > s.estimated_delivery_time) AND (s.status != 3), 1, NULL)) AS delayed_deliveries
FROM db_streaming_logistics.drivers AS d
INNER JOIN db_streaming_logistics.users AS u ON d.user_id = u.user_id
LEFT JOIN db_streaming_logistics.shipments AS s ON d.driver_id = s.driver_id
GROUP BY
    d.driver_id,
    u.full_name,
    s.created_at;

-- 3. Real-time Business Analytics Materialized View
DROP TABLE IF EXISTS db_streaming_logistics.mv_business_metrics;
CREATE MATERIALIZED VIEW db_streaming_logistics.mv_business_metrics
ENGINE = SummingMergeTree()
ORDER BY (hour)
POPULATE AS
SELECT
    toStartOfHour(o.created_at) AS hour,
    COUNT() AS total_orders,
    countIf(o.status = 4) AS completed_orders,
    countIf(o.status = 3) AS active_orders,
    SUM(p.amount) AS revenue,
    AVG(p.amount) AS avg_order_value,
    (countIf(o.status = 4) / COUNT()) * 100 AS completion_rate
FROM db_streaming_logistics.orders AS o
LEFT JOIN db_streaming_logistics.payments AS p ON o.order_id = p.order_id
GROUP BY hour;

-- 4. Real-time Alert System Materialized View
DROP TABLE IF EXISTS db_streaming_logistics.mv_realtime_alerts;
CREATE MATERIALIZED VIEW db_streaming_logistics.mv_realtime_alerts
ENGINE = MergeTree()
ORDER BY (alert_type, created_at)
POPULATE AS
SELECT
    'Delayed Delivery' AS alert_type,
    o.order_id AS order_id,
    o.user_id AS user_id,
    s.driver_id AS driver_id,
    s.estimated_delivery_time AS estimated_delivery_time,
    s.created_at AS created_at,
    now() AS current_time,
    dateDiff('minute', s.estimated_delivery_time, now()) AS delay_minutes
FROM db_streaming_logistics.orders AS o
INNER JOIN db_streaming_logistics.shipments AS s ON o.order_id = s.order_id
WHERE (s.status = 2) AND (now() > s.estimated_delivery_time)

UNION ALL

SELECT
    'Payment Failed' AS alert_type,
    o.order_id AS order_id,
    o.user_id AS user_id,
    NULL AS driver_id,
    p.payment_date AS estimated_delivery_time,
    p.payment_date AS created_at,  -- Sử dụng payment_date thay vì created_at
    now() AS current_time,
    NULL AS delay_minutes
FROM db_streaming_logistics.orders AS o
INNER JOIN db_streaming_logistics.payments AS p ON o.order_id = p.order_id
WHERE p.payment_status = 3;

-- 5. Real-time Location Tracking Materialized View
DROP TABLE IF EXISTS db_streaming_logistics.mv_location_tracking;
CREATE MATERIALIZED VIEW db_streaming_logistics.mv_location_tracking
ENGINE = MergeTree()
ORDER BY (shipment_id, created_at)
POPULATE AS
SELECT
    s.shipment_id AS shipment_id,
    s.order_id AS order_id,
    s.driver_id AS driver_id,
    s.created_at AS created_at,
    d.full_name AS driver_name,
    s.current_location AS current_location,
    o.pickup_address AS pickup_address,
    o.delivery_address AS delivery_address,
    s.estimated_delivery_time AS estimated_delivery_time,
    o.status AS order_status, -- Enum order status
    s.status AS shipment_status -- Enum shipment status
FROM db_streaming_logistics.shipments AS s
JOIN db_streaming_logistics.orders AS o ON s.order_id = o.order_id
JOIN db_streaming_logistics.drivers AS dr ON s.driver_id = dr.driver_id
JOIN db_streaming_logistics.users AS d ON dr.user_id = d.user_id
WHERE s.status = 2; -- status = 'in_transit' = 2