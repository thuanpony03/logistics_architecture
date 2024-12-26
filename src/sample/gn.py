from faker import Faker
import random
from datetime import datetime, timedelta
import pymysql
from werkzeug.security import generate_password_hash

# Khởi tạo Faker với locale tiếng Việt
fake = Faker(['vi_VN'])

# Cấu hình Database
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'ponydasierra',
    'database': 'db_logistics',
    'charset': 'utf8mb4'
}

# Số lượng bản ghi cần tạo
NUM_USERS = 100
NUM_DRIVERS = 20
NUM_ORDERS = 200
BATCH_SIZE = 50


def create_connection():
    """Tạo kết nối đến MySQL database"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("✓ Kết nối database thành công!")
        return connection
    except Exception as e:
        print(f"Lỗi kết nối database: {e}")
        raise


def generate_address():
    """Tạo địa chỉ ngẫu nhiên"""
    return f"{random.randint(1, 200)} {fake.street_name()}, {fake.city()}"


def generate_license_plate():
    """Tạo biển số xe ngẫu nhiên"""
    cities = ['29', '30', '31', '32', '33', '40', '50', '51']
    letters = 'ABCDEFGHJKLMNPRSTUVWXYZ'
    return f"{random.choice(cities)}-{random.choice(letters)}{random.choice(letters)} {random.randint(10000, 99999)}"


def create_sample_data():
    """Tạo dữ liệu mẫu cho toàn bộ database"""
    connection = create_connection()
    cursor = connection.cursor()

    try:
        # 1. Tạo Users và Drivers
        print("\nBắt đầu tạo Users và Drivers...")
        user_ids = []
        driver_user_ids = []

        for i in range(NUM_USERS):
            # Tạo user với email không trùng lặp
            is_driver = i < NUM_DRIVERS
            role = 'driver' if is_driver else 'user'

            # Tạo email không trùng lặp bằng cách thêm số ngẫu nhiên
            email = f"user{i}_{random.randint(1000, 9999)}@{fake.domain_name()}"
            phone = f"0{random.randint(300000000, 999999999)}"

            cursor.execute("""
                INSERT INTO Users (full_name, email, password_hash, phone_number, address, role)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                fake.name(),
                email,
                generate_password_hash('password123'),
                phone,
                generate_address(),
                role
            ))

            user_id = cursor.lastrowid
            user_ids.append(user_id)

            if is_driver:
                driver_user_ids.append(user_id)
                # Tạo driver
                cursor.execute("""
                    INSERT INTO Drivers (user_id, vehicle_license_plate, vehicle_type, vehicle_year)
                    VALUES (%s, %s, %s, %s)
                """, (
                    user_id,
                    generate_license_plate(),
                    random.choice(['Xe tải nhỏ', 'Xe van', 'Xe máy']),
                    random.randint(2018, 2023)
                ))

            if (i + 1) % BATCH_SIZE == 0:
                connection.commit()
                print(f"✓ Đã tạo {i + 1}/{NUM_USERS} users")

        connection.commit()
        print(f"✓ Đã tạo xong {NUM_USERS} users và {NUM_DRIVERS} drivers")
        # 2. Tạo Orders và các bảng liên quan
        print("\nBắt đầu tạo Orders và dữ liệu liên quan...")

        current_time = datetime(2024, 12, 24, 12, 39, 7)

        for i in range(NUM_ORDERS):
            # Tạo order
            user_id = random.choice(user_ids)
            created_at = fake.date_time_between(
                start_date='-1m',
                end_date=current_time
            )
            delivery_time = created_at + timedelta(days=random.randint(1, 3))

            time_diff = current_time - created_at
            if time_diff.total_seconds() < 7200:  # 2 giờ
                status = 'processing'
            elif time_diff.total_seconds() < 21600:  # 6 giờ
                status = 'accepted'
            elif time_diff.total_seconds() < 172800:  # 48 giờ
                status = 'in_transit'
            else:
                status = 'delivered'

            cursor.execute("""
                        INSERT INTO Orders (user_id, pickup_address, delivery_address,
                                          package_description, package_weight, delivery_time, status, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                user_id,
                generate_address(),
                generate_address(),
                f"Gói hàng #{i + 1}",
                round(random.uniform(0.1, 30.0), 2),
                delivery_time,
                status,
                created_at
            ))

            order_id = cursor.lastrowid

            # Tạo shipment nếu đơn không ở trạng thái processing
            if status != 'processing':
                shipment_status = {
                    'accepted': 'assigned',
                    'in_transit': 'in_transit',
                    'delivered': 'completed'
                }[status]

                cursor.execute("""
                            INSERT INTO Shipments (order_id, driver_id, current_location,
                                                 estimated_delivery_time, status)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                    order_id,
                    random.randint(1, NUM_DRIVERS),
                    generate_address(),
                    delivery_time,
                    shipment_status
                ))

            # Tạo payment với thông tin chi tiết hơn
            base_amount = random.randint(50, 200) * 1000  # Số tiền cơ bản
            distance_fee = random.randint(5, 20) * 5000  # Phí khoảng cách
            weight_fee = random.randint(1, 10) * 10000  # Phí trọng lượng
            total_amount = base_amount + distance_fee + weight_fee

            cursor.execute("""
                        INSERT INTO Payments (order_id, amount, payment_method, payment_status, payment_date)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                order_id,
                total_amount,
                random.choice(['credit_card', 'e_wallet', 'bank_transfer']),
                'completed' if status == 'delivered' else 'pending',
                created_at + timedelta(minutes=random.randint(1, 10))  # Thời gian thanh toán sau khi tạo đơn
            ))

            # Tạo nhiều notifications theo trạng thái đơn hàng
            notifications = []

            # Notification khi tạo đơn
            notifications.append((
                user_id,
                f"Đơn hàng #{order_id} đã được tạo thành công",
                created_at
            ))

            if status != 'processing':
                # Notification khi xác nhận đơn
                notifications.append((
                    user_id,
                    f"Đơn hàng #{order_id} đã được xác nhận và đang tìm tài xế",
                    created_at + timedelta(minutes=random.randint(5, 30))
                ))

            if status in ['in_transit', 'delivered']:
                # Notification khi bắt đầu giao hàng
                notifications.append((
                    user_id,
                    f"Đơn hàng #{order_id} đang được giao đến bạn",
                    created_at + timedelta(hours=random.randint(1, 4))
                ))

            if status == 'delivered':
                # Notification khi giao hàng thành công
                notifications.append((
                    user_id,
                    f"Đơn hàng #{order_id} đã được giao thành công",
                    delivery_time
                ))

                # Notification yêu cầu đánh giá
                notifications.append((
                    user_id,
                    f"Hãy đánh giá trải nghiệm giao hàng của bạn với đơn hàng #{order_id}",
                    delivery_time + timedelta(hours=1)
                ))

            # Insert tất cả notifications
            for notif in notifications:
                cursor.execute("""
                            INSERT INTO Notifications (user_id, message, notification_date)
                            VALUES (%s, %s, %s)
                        """, notif)

            if (i + 1) % BATCH_SIZE == 0:
                connection.commit()
                print(f"✓ Đã tạo {i + 1}/{NUM_ORDERS} orders")

        connection.commit()
        print(f"✓ Đã tạo xong {NUM_ORDERS} orders và dữ liệu liên quan")

    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    print(f"Bắt đầu tạo dữ liệu mẫu - {datetime.utcnow()}")
    print(f"Current User: thuanpony03")
    create_sample_data()
    print("\n✓ Hoàn thành!")