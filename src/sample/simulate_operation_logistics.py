from faker import Faker
import random
from datetime import datetime, timedelta
import pymysql
from concurrent.futures import ThreadPoolExecutor
import time

# Khởi tạo Faker với locale tiếng Việt
fake = Faker(['vi_VN'])

# Thông tin kết nối database
db_params = {
    'host': 'localhost',
    'user': 'root',
    'password': 'ponydasierra',
    'database': 'db_logistics'
}


class LogisticsSimulator:
    def __init__(self):
        self.conn = pymysql.connect(**db_params)
        self.cursor = self.conn.cursor()

    def __del__(self):
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

    def get_active_users(self):
        """Lấy danh sách users đang hoạt động"""
        self.cursor.execute("SELECT user_id FROM Users WHERE role = 'user'")
        return [row[0] for row in self.cursor.fetchall()]

    def get_available_drivers(self):
        """Lấy danh sách tài xế đang rảnh"""
        self.cursor.execute("""
            SELECT d.driver_id 
            FROM Drivers d
            LEFT JOIN Shipments s ON d.driver_id = s.driver_id 
            WHERE s.driver_id IS NULL OR s.status = 'completed'
            GROUP BY d.driver_id
        """)
        return [row[0] for row in self.cursor.fetchall()]

    def create_realistic_order(self, user_id):
        """Tạo đơn hàng với thời gian và trạng thái thực tế"""
        try:
            # Tạo thông tin đơn hàng
            current_time = datetime.now()
            order_date = current_time - timedelta(hours=random.randint(0, 72))

            # Tính toán thời gian dự kiến dựa trên khoảng cách
            distance = random.uniform(1, 50)  # Khoảng cách km
            estimated_hours = distance * 0.1 + random.uniform(0.5, 2)  # 0.1 giờ/km + thời gian xử lý
            delivery_time = order_date + timedelta(hours=estimated_hours)

            # Xác định trạng thái dựa trên thời gian
            if order_date > current_time - timedelta(minutes=30):
                status = 'processing'
            elif order_date > current_time - timedelta(hours=2):
                status = 'accepted'
            elif delivery_time > current_time:
                status = 'in_transit'
            else:
                status = 'delivered'

            # Tạo chi tiết đơn hàng
            self.cursor.execute("""
                INSERT INTO Orders (user_id, pickup_address, delivery_address,
                                  package_description, package_weight, delivery_time,
                                  status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                user_id,
                fake.address()[:255],
                fake.address()[:255],
                f"Gói hàng {random.choice(['nhỏ', 'vừa', 'lớn'])} - {fake.word()}",
                round(random.uniform(0.1, 50.0), 2),
                delivery_time,
                status,
                order_date
            ))

            order_id = self.cursor.lastrowid

            # Tạo shipment nếu đơn đã được chấp nhận
            if status != 'processing':
                available_drivers = self.get_available_drivers()
                if available_drivers:
                    driver_id = random.choice(available_drivers)
                    shipment_status = 'completed' if status == 'delivered' else 'in_transit'

                    self.cursor.execute("""
                        INSERT INTO Shipments (order_id, driver_id, current_location,
                                             estimated_delivery_time, status)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        order_id,
                        driver_id,
                        self.generate_location_based_on_status(status),
                        delivery_time,
                        shipment_status
                    ))

            # Tạo payment với trạng thái phù hợp
            amount = round(random.uniform(50000, 500000))
            payment_status = 'completed' if status == 'delivered' else 'pending'

            self.cursor.execute("""
                INSERT INTO Payments (order_id, amount, payment_method,
                                    payment_status, payment_date)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                order_id,
                amount,
                random.choice(['credit_card', 'e_wallet', 'bank_transfer']),
                payment_status,
                order_date if payment_status == 'completed' else None
            ))

            # Tạo thông báo theo trạng thái
            self.create_notifications_for_order(order_id, user_id, status)

            self.conn.commit()
            return order_id

        except Exception as e:
            self.conn.rollback()
            print(f"Lỗi khi tạo đơn hàng: {e}")
            return None

    def generate_location_based_on_status(self, status):
        """Tạo vị trí dựa trên trạng thái đơn hàng"""
        if status == 'processing':
            return None
        elif status == 'accepted':
            return f"Trung tâm phân loại {fake.city()}"
        elif status == 'in_transit':
            return f"Đang giao tại {fake.street_address()}"
        else:
            return "Đã giao thành công"

    def create_notifications_for_order(self, order_id, user_id, status):
        """Tạo thông báo theo trạng thái đơn hàng"""
        notifications = {
            'processing': f"Đơn hàng #{order_id} đã được tạo và đang chờ xử lý",
            'accepted': f"Đơn hàng #{order_id} đã được xác nhận và sẽ sớm được giao",
            'in_transit': f"Đơn hàng #{order_id} đang được giao đến bạn",
            'delivered': f"Đơn hàng #{order_id} đã được giao thành công"
        }

        self.cursor.execute("""
            INSERT INTO Notifications (user_id, message, notification_date)
            VALUES (%s, %s, %s)
        """, (user_id, notifications[status], datetime.now()))

    def simulate_live_system(self, duration_minutes=60, order_frequency=5):
        """Mô phỏng hệ thống đang hoạt động"""
        start_time = datetime.now()
        active_users = self.get_active_users()

        if not active_users:
            print("Không tìm thấy user nào trong hệ thống!")
            return

        print(f"Bắt đầu mô phỏng hệ thống trong {duration_minutes} phút...")
        print(f"Số lượng user hoạt động: {len(active_users)}")

        while (datetime.now() - start_time).total_seconds() < duration_minutes * 60:
            # Tạo đơn hàng mới
            user_id = random.choice(active_users)
            order_id = self.create_realistic_order(user_id)

            if order_id:
                print(f"Tạo đơn hàng mới: #{order_id}")

            # Cập nhật trạng thái các đơn hàng hiện có
            self.update_existing_orders()

            # Đợi một khoảng thời gian trước khi tạo đơn hàng tiếp theo
            time.sleep(random.uniform(1, order_frequency))

    def update_existing_orders(self):
        """Cập nhật trạng thái các đơn hàng hiện có"""
        try:
            # Lấy các đơn hàng đang trong quá trình xử lý
            self.cursor.execute("""
                SELECT order_id, status, created_at, delivery_time, user_id
                FROM Orders
                WHERE status != 'delivered' AND status != 'cancelled'
            """)

            active_orders = self.cursor.fetchall()

            for order in active_orders:
                order_id, status, created_at, delivery_time, user_id = order
                current_time = datetime.now()
                order_age = current_time - created_at

                # Cập nhật trạng thái dựa trên thời gian
                new_status = None
                if status == 'processing' and order_age > timedelta(minutes=30):
                    new_status = 'accepted'
                elif status == 'accepted' and order_age > timedelta(hours=1):
                    new_status = 'in_transit'
                elif status == 'in_transit' and current_time > delivery_time:
                    new_status = 'delivered'

                if new_status:
                    # Cập nhật trạng thái đơn hàng
                    self.cursor.execute("""
                        UPDATE Orders SET status = %s WHERE order_id = %s
                    """, (new_status, order_id))

                    # Cập nhật shipment nếu có
                    if new_status in ['in_transit', 'delivered']:
                        self.cursor.execute("""
                            UPDATE Shipments 
                            SET status = %s, current_location = %s 
                            WHERE order_id = %s
                        """, (
                            'completed' if new_status == 'delivered' else 'in_transit',
                            self.generate_location_based_on_status(new_status),
                            order_id
                        ))

                    # Cập nhật payment nếu đã giao hàng
                    if new_status == 'delivered':
                        self.cursor.execute("""
                            UPDATE Payments 
                            SET payment_status = 'completed', payment_date = %s
                            WHERE order_id = %s
                        """, (current_time, order_id))

                    # Tạo thông báo cho user
                    self.create_notifications_for_order(
                        order_id,
                        user_id,
                        new_status
                    )

                    print(f"Cập nhật đơn hàng #{order_id}: {new_status}")

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            print(f"Lỗi khi cập nhật đơn hàng: {e}")


def main():
    try:
        simulator = LogisticsSimulator()

        # Mô phỏng hệ thống trong 60 phút, với tần suất đơn hàng mới mỗi 3-5 phút
        simulator.simulate_live_system(duration_minutes=60, order_frequency=5)

    except Exception as e:
        print(f"Lỗi trong quá trình mô phỏng: {e}")


if __name__ == "__main__":
    main()