from faker import Faker
import random
from datetime import datetime, timedelta
import pymysql
from werkzeug.security import generate_password_hash
import re

# Khởi tạo Faker với locale tiếng Việt
fake = Faker(['vi_VN'])

# Thông tin kết nối database
db_params = {
    'host': 'localhost',
    'user': 'root',
    'password': 'ponydasierra',
    'database': 'db_logistics'
}

# Các hằng số cho việc tạo dữ liệu
NUM_USERS = 1000
NUM_DRIVERS = 200
NUM_ORDERS = 5000
BATCH_SIZE = 100


def create_connection():
    """Tạo kết nối đến database"""
    try:
        conn = pymysql.connect(**db_params)
        return conn
    except Exception as e:
        print(f"Lỗi kết nối database: {e}")
        raise


def remove_accents(input_str):
    """Chuyển chuỗi tiếng Việt có dấu thành không dấu"""
    s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ'
    s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYy'
    s = ''
    for c in input_str:
        if c in s1:
            s += s0[s1.index(c)]
        else:
            s += c
    return s


# Định nghĩa dữ liệu địa điểm cố định cho Việt Nam
VIETNAM_LOCATIONS = {
    'Hà Nội': {
        'districts': {
            'Hoàn Kiếm': ['Hàng Bông', 'Lý Thái Tổ', 'Tràng Tiền', 'Hàng Gai', 'Hàng Đào'],
            'Ba Đình': ['Liễu Giai', 'Kim Mã', 'Ngọc Hà', 'Đội Cấn', 'Quán Thánh'],
            'Đống Đa': ['Láng Hạ', 'Chợ Dừa', 'Khâm Thiên', 'Phương Mai', 'Trung Tự'],
            'Cầu Giấy': ['Dịch Vọng', 'Mai Dịch', 'Yên Hòa', 'Trung Hòa', 'Quan Hoa'],
            'Hai Bà Trưng': ['Bách Khoa', 'Quỳnh Mai', 'Thanh Nhàn', 'Bạch Mai', 'Vĩnh Tuy']
        }
    },
    'Hồ Chí Minh': {
        'districts': {
            'Quận 1': ['Bến Nghé', 'Bến Thành', 'Nguyễn Thái Bình', 'Phạm Ngũ Lão', 'Tân Định'],
            'Quận 3': ['Võ Thị Sáu', 'Nguyễn Thị Minh Khai', 'Nguyễn Đình Chiểu', 'Phường 9', 'Phường 10'],
            'Quận 7': ['Tân Thuận Đông', 'Tân Thuận Tây', 'Tân Kiểng', 'Tân Hưng', 'Tân Phong'],
            'Bình Thạnh': ['Phường 1', 'Phường 2', 'Phường 3', 'Phường 11', 'Phường 12'],
            'Tân Bình': ['Phường 1', 'Phường 2', 'Phường 3', 'Phường 4', 'Phường 5']
        }
    }
}


def generate_address():
    """
    Tạo địa chỉ ngẫu nhiên với format:
    Số nhà + Tên đường + Phường/Xã + Quận/Huyện + Thành phố
    """
    # Chọn thành phố ngẫu nhiên
    city = random.choice(list(VIETNAM_LOCATIONS.keys()))

    # Chọn quận/huyện của thành phố đó
    district = random.choice(list(VIETNAM_LOCATIONS[city]['districts'].keys()))

    # Chọn phường/đường của quận đó
    street = random.choice(VIETNAM_LOCATIONS[city]['districts'][district])

    # Tạo số nhà ngẫu nhiên
    house_number = random.randint(1, 200)

    # Tạo địa chỉ đầy đủ
    full_address = f"Số {house_number} {street}, {district}, {city}"

    return {
        'city': city,
        'district': district,
        'street': street,
        'house_number': house_number,
        'full_address': full_address
    }


def get_random_route():
    """
    Tạo tuyến đường ngẫu nhiên giữa hai địa điểm trong cùng thành phố
    """
    # Chọn thành phố
    city = random.choice(list(VIETNAM_LOCATIONS.keys()))
    districts = list(VIETNAM_LOCATIONS[city]['districts'].keys())

    # Chọn quận/huyện xuất phát và đích khác nhau
    pickup_district = random.choice(districts)
    delivery_district = random.choice([d for d in districts if d != pickup_district])

    # Tạo địa chỉ đón và trả
    pickup = {
        'city': city,
        'district': pickup_district,
        'street': random.choice(VIETNAM_LOCATIONS[city]['districts'][pickup_district]),
        'house_number': random.randint(1, 200)
    }

    delivery = {
        'city': city,
        'district': delivery_district,
        'street': random.choice(VIETNAM_LOCATIONS[city]['districts'][delivery_district]),
        'house_number': random.randint(1, 200)
    }

    # Tạo địa chỉ đầy đủ
    pickup['full_address'] = f"Số {pickup['house_number']} {pickup['street']}, {pickup['district']}, {pickup['city']}"
    delivery[
        'full_address'] = f"Số {delivery['house_number']} {delivery['street']}, {delivery['district']}, {delivery['city']}"

    return pickup, delivery


def create_unique_email(full_name):
    """Tạo email độc nhất từ tên người dùng"""
    # Chuyển tên thành dạng không dấu và lowercase
    name_normalized = remove_accents(full_name.lower())

    # Loại bỏ các ký tự đặc biệt và khoảng trắng
    name_cleaned = re.sub(r'[^a-z0-9]', '', name_normalized)

    # Thêm số ngẫu nhiên để tăng tính độc nhất
    random_number = random.randint(1000, 9999)

    # Tạo email với domain ngẫu nhiên
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
    domain = random.choice(domains)

    return f"{name_cleaned}{random_number}@{domain}"


def create_unique_phone():
    """Tạo số điện thoại Việt Nam độc nhất"""
    # Các đầu số phổ biến ở Việt Nam
    prefixes = ['032', '033', '034', '035', '036', '037', '038', '039',  # Viettel
                '070', '079', '077', '076', '078',  # Mobifone
                '081', '082', '083', '084', '085']  # Vinaphone

    prefix = random.choice(prefixes)
    number = ''.join([str(random.randint(0, 9)) for _ in range(7)])

    return f"{prefix}{number}"


def create_users(cursor, num_users):
    """Tạo dữ liệu người dùng"""
    user_ids = []
    created_emails = set()  # Để kiểm tra email trùng lặp
    created_phones = set()  # Để kiểm tra số điện thoại trùng lặp

    for i in range(num_users):
        attempts = 0
        while attempts < 3:  # Giới hạn số lần thử để tránh vòng lặp vô hạn
            try:
                # Tạo thông tin người dùng
                full_name = fake.name()
                email = create_unique_email(full_name)
                phone = create_unique_phone()

                # Kiểm tra trùng lặp
                if email in created_emails or phone in created_phones:
                    attempts += 1
                    continue

                address = get_random_route()[0]['full_address']
                role = 'user'  # Mặc định là user thường
                password_hash = generate_password_hash('password123')

                cursor.execute("""
                    INSERT INTO Users (full_name, email, password_hash, phone_number, 
                                     address, role)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (full_name, email, password_hash, phone, address, role))

                user_ids.append(cursor.lastrowid)
                created_emails.add(email)
                created_phones.add(phone)

                if i % BATCH_SIZE == 0 and i > 0:
                    print(f"Đã tạo {i} users")
                    cursor.connection.commit()

                break  # Thoát khỏi vòng lặp while nếu insert thành công

            except pymysql.Error as e:
                print(f"Lỗi khi tạo user {full_name}: {e}")
                attempts += 1

        if attempts == 3:
            print(f"Không thể tạo user sau 3 lần thử")
            continue

    cursor.connection.commit()
    return user_ids


def create_drivers(cursor, user_ids, num_drivers):
    """Tạo dữ liệu tài xế"""
    driver_ids = []
    vehicle_types = ['Xe tải nhỏ', 'Xe tải vừa', 'Xe tải lớn', 'Xe máy', 'Xe van']

    # Chọn ngẫu nhiên một số user để làm tài xế
    driver_user_ids = random.sample(user_ids, min(num_drivers, len(user_ids)))

    for user_id in driver_user_ids:
        try:
            # Cập nhật role của user thành driver
            cursor.execute("UPDATE Users SET role = 'driver' WHERE user_id = %s", (user_id,))

            # Tạo thông tin xe
            vehicle_type = random.choice(vehicle_types)
            vehicle_year = random.randint(2015, 2024)

            # Tạo biển số xe theo định dạng Việt Nam
            province_codes = ['29', '30', '31', '32', '33', '34', '35', '36', '37', '38']
            series = random.choice('ABCDEFGHK')
            numbers = f"{random.randint(10000, 99999)}"
            license_plate = f"{random.choice(province_codes)}{series}{numbers}"

            cursor.execute("""
                INSERT INTO Drivers (user_id, vehicle_license_plate, vehicle_type, 
                                   vehicle_year)
                VALUES (%s, %s, %s, %s)
            """, (user_id, license_plate, vehicle_type, vehicle_year))

            driver_ids.append(cursor.lastrowid)

            if len(driver_ids) % BATCH_SIZE == 0:
                print(f"Đã tạo {len(driver_ids)} drivers")
                cursor.connection.commit()

        except Exception as e:
            print(f"Lỗi khi tạo driver cho user {user_id}: {e}")
            continue

    cursor.connection.commit()
    return driver_ids


def create_orders(cursor, user_ids, driver_ids, num_orders):
    """Tạo dữ liệu đơn hàng và vận chuyển"""
    statuses = ['processing', 'accepted', 'in_transit', 'delivered']
    payment_methods = ['credit_card', 'e_wallet', 'bank_transfer']

    for i in range(num_orders):
        # Tạo thông tin đơn hàng
        user_id = random.choice(user_ids)
        pickup_address = fake.address()[:255]
        delivery_address = fake.address()[:255]
        package_desc = fake.text(max_nb_chars=200)
        weight = round(random.uniform(0.1, 100.0), 2)

        # Tạo thời gian theo trình tự logic
        created_at = fake.date_time_between(start_date='-3m')
        delivery_time = created_at + timedelta(days=random.randint(1, 7))

        status = random.choice(statuses)

        try:
            # Tạo đơn hàng
            cursor.execute("""
                INSERT INTO Orders (user_id, pickup_address, delivery_address,
                                  package_description, package_weight, delivery_time,
                                  status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, pickup_address, delivery_address, package_desc,
                  weight, delivery_time, status, created_at))

            order_id = cursor.lastrowid

            # Tạo thông tin vận chuyển nếu đơn hàng đã được chấp nhận
            if status != 'processing':
                driver_id = random.choice(driver_ids)
                current_location = fake.address()[:255]
                estimated_delivery = delivery_time
                shipment_status = 'completed' if status == 'delivered' else 'in_transit'

                cursor.execute("""
                    INSERT INTO Shipments (order_id, driver_id, current_location,
                                         estimated_delivery_time, status)
                    VALUES (%s, %s, %s, %s, %s)
                """, (order_id, driver_id, current_location, estimated_delivery,
                      shipment_status))

            # Tạo thông tin thanh toán
            amount = round(weight * random.uniform(20000, 50000), 2)
            payment_method = random.choice(payment_methods)
            payment_status = 'completed' if status == 'delivered' else 'pending'

            cursor.execute("""
                INSERT INTO Payments (order_id, amount, payment_method,
                                    payment_status, payment_date)
                VALUES (%s, %s, %s, %s, %s)
            """, (order_id, amount, payment_method, payment_status, created_at))

            # Tạo thông báo
            if status != 'processing':
                notification_msg = f"Đơn hàng #{order_id} đã {status}"
                cursor.execute("""
                    INSERT INTO Notifications (user_id, message)
                    VALUES (%s, %s)
                """, (user_id, notification_msg))

            if i % BATCH_SIZE == 0:
                print(f"Đã tạo {i} orders")
                cursor.connection.commit()

        except Exception as e:
            print(f"Lỗi khi tạo order {i}: {e}")
            continue


def main():
    """Hàm chính để chạy quá trình tạo dữ liệu"""
    conn = create_connection()
    cursor = conn.cursor()

    try:
        print("Bắt đầu tạo dữ liệu mẫu...")

        print("\nTạo users...")
        user_ids = create_users(cursor, NUM_USERS)
        print(f"Đã tạo {len(user_ids)} users")

        print("\nTạo drivers...")
        driver_ids = create_drivers(cursor, user_ids, NUM_DRIVERS)
        print(f"Đã tạo {len(driver_ids)} drivers")

        print("\nTạo orders và dữ liệu liên quan...")
        create_orders(cursor, user_ids, driver_ids, NUM_ORDERS)
        print(f"Đã tạo {NUM_ORDERS} orders")

        conn.commit()
        print("\nHoàn thành tạo dữ liệu mẫu!")

    except Exception as e:
        conn.rollback()
        print(f"Lỗi: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()