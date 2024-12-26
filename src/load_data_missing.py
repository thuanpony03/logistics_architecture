import pymysql
from datetime import datetime

# Kết nối đến cơ sở dữ liệu MySQL
connection = pymysql.connect(
    host='localhost',  # Đổi thành địa chỉ host của bạn
    user='root',  # Đổi thành username MySQL của bạn
    password='ponydasierra',  # Đổi thành mật khẩu MySQL của bạn
    database='db_logistics',  # Đổi thành tên database của bạn
)

cursor = connection.cursor()

# Dữ liệu mẫu cho bảng Drivers
drivers_data = [
    (1, 1, 'XYZ-7598', 'car', 2013),
    (2, 2, 'XYZ-8290', 'car', 2015),
    (3, 3, 'XYZ-6799', 'bike', 2018),
    (4, 4, 'XYZ-6274', 'car', 2020),
    (5, 5, 'XYZ-4392', 'truck', 2012),
]

# Dữ liệu mẫu cho bảng Payments
payments_data = [
    (1, 1, 100.50, 'credit_card', 'completed', '2024-12-01 10:00:00'),
    (2, 2, 50.75, 'e_wallet', 'completed', '2024-12-02 12:30:00'),
    (3, 3, 200.00, 'bank_transfer', 'pending', '2024-12-03 15:45:00'),
    (4, 4, 75.25, 'credit_card', 'failed', '2024-12-04 18:20:00'),
    (5, 5, 300.40, 'e_wallet', 'completed', '2024-12-05 20:10:00'),
]

# Dữ liệu mẫu cho bảng Notifications
notifications_data = [
    (1, 1, 'Your order has been shipped.', '2024-12-01 11:00:00'),
    (2, 2, 'Your payment has been received.', '2024-12-02 13:30:00'),
    (3, 3, 'Your order is out for delivery.', '2024-12-03 16:45:00'),
    (4, 4, 'Your order has been delivered.', '2024-12-04 19:20:00'),
    (5, 5, 'Your payment has been confirmed.', '2024-12-05 21:10:00'),
]

# Chèn dữ liệu vào bảng Drivers
cursor.executemany("""
    INSERT INTO Drivers (driver_id, user_id, vehicle_license_plate, vehicle_type, vehicle_year)
    VALUES (%s, %s, %s, %s, %s)
""", drivers_data)

# Chèn dữ liệu vào bảng Payments
cursor.executemany("""
    INSERT INTO Payments (payment_id, order_id, amount, payment_method, payment_status, payment_date)
    VALUES (%s, %s, %s, %s, %s, %s)
""", payments_data)

# Chèn dữ liệu vào bảng Notifications
cursor.executemany("""
    INSERT INTO Notifications (notification_id, user_id, message, notification_date)
    VALUES (%s, %s, %s, %s)
""", notifications_data)

# Lưu thay đổi và đóng kết nối
connection.commit()
cursor.close()
connection.close()

print("Dữ liệu mẫu đã được chèn thành công!")
