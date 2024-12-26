from faker import Faker
import random
from datetime import datetime, timedelta
import pymysql
from werkzeug.security import generate_password_hash
from enum import Enum
import uuid

# Initialize Faker with Vietnamese locale
fake = Faker(['vi_VN'])

# Database connection parameters
db_params = {
    'host': 'localhost',
    'user': 'root',
    'password': 'ponydasierra',
    'db': 'db_logistics',
    'port': 3306
}

# Constants for data generation
NUM_USERS = 10000
NUM_ORDERS = 50000
NUM_WAREHOUSES = 20
NUM_PACKAGES = 50
BATCH_SIZE = 1000


class PaymentStatus(Enum):
    PENDING = 1
    COMPLETED = 2
    FAILED = 3
    REFUNDED = 4


class PaymentMethod(Enum):
    CASH = 1
    CREDIT_CARD = 2
    BANK_TRANSFER = 3
    E_WALLET = 4


def check_database_connection():
    try:
        conn = pymysql.connect(**db_params)
        conn.close()
        return True
    except Exception as e:
        print(f"Database connection test failed: {e}")
        return False


def create_roles(conn):
    """Create roles with claims"""
    roles = [
        ('Admin', ['FullAccess', 'ManageUsers', 'ViewReports']),
        ('User', ['CreateOrders', 'ViewOwnOrders']),
        ('Driver', ['ViewAssignedOrders', 'UpdateDeliveryStatus'])
    ]
    role_ids = []

    try:
        with conn.cursor() as cur:
            for role_name, claims in roles:
                role_id = str(uuid.uuid4())
                cur.execute("""
                    INSERT INTO AspNetRoles (Id, Name, NormalizedName, ConcurrencyStamp)
                    VALUES (%s, %s, %s, %s)
                """, (role_id, role_name, role_name.upper(), str(uuid.uuid4())))

                # Create role claims
                for claim in claims:
                    cur.execute("""
                        INSERT INTO AspNetRoleClaims (RoleId, ClaimType, ClaimValue)
                        VALUES (%s, %s, %s)
                    """, (role_id, 'Permission', claim))

                role_ids.append((role_id, role_name))

        conn.commit()
        return role_ids
    except Exception as e:
        conn.rollback()
        print(f"Error creating roles: {e}")
        raise


def create_users(conn, num_users, role_ids):
    """Create users with related data"""
    user_ids = []
    try:
        with conn.cursor() as cur:
            for i in range(num_users):
                # Determine role distribution
                role_type = "User"
                if i < num_users * 0.05:  # 5% Admins
                    role_type = "Admin"
                elif i < num_users * 0.15:  # 10% Drivers
                    role_type = "Driver"

                # Create user
                user_id = str(uuid.uuid4())
                username = f"user_{str(i + 1).zfill(6)}"[:256]
                email = f"{username}@example.com"[:256]

                cur.execute("""
                    INSERT INTO AspNetUsers (
                        Id, Discriminator, FullName, Address, ProfilePicUrl,
                        UserName, NormalizedUserName, Email, NormalizedEmail,
                        EmailConfirmed, PasswordHash, SecurityStamp,
                        ConcurrencyStamp, PhoneNumber, PhoneNumberConfirmed,
                        TwoFactorEnabled, LockoutEnabled, AccessFailedCount
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    user_id, 'ApplicationUser', fake.name(), fake.address(),
                    f"https://api.dicebear.com/7.x/avataaars/svg?seed={user_id}",
                    username, username.upper(), email, email.upper(),
                    1, generate_password_hash('Password123!'),
                    str(uuid.uuid4()), str(uuid.uuid4()), fake.phone_number(),
                    1, 0, 1, 0
                ))

                # Assign role
                role_id = next(rid for rid, rname in role_ids if rname == role_type)
                cur.execute("""
                    INSERT INTO AspNetUserRoles (UserId, RoleId)
                    VALUES (%s, %s)
                """, (user_id, role_id))

                # Create user claims
                cur.execute("""
                    INSERT INTO AspNetUserClaims (UserId, ClaimType, ClaimValue)
                    VALUES (%s, %s, %s)
                """, (user_id, 'UserType', role_type))

                # Create OTP
                cur.execute("""
                    INSERT INTO UserOTPs (UserId, OTP, Expiration)
                    VALUES (%s, %s, %s)
                """, (
                    user_id,
                    str(random.randint(100000, 999999)),
                    datetime.now() + timedelta(minutes=15)
                ))

                # Create driver if applicable
                if role_type == "Driver":
                    cur.execute("""
                        INSERT INTO Drivers (
                            UserId, FullName, NameVehicle, MFGDate,
                            VehicleColor, LicensePlate, IsActive
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        user_id,
                        fake.name(),
                        f"{random.choice(['Toyota', 'Honda', 'Ford'])} {random.choice(['Truck', 'Van'])}",
                        fake.date_between(start_date='-5y').strftime('%Y-%m-%d'),
                        fake.color_name(),
                        f"{random.randint(10, 99)}{random.choice('ABCDEFGHK')}-{random.randint(10000, 99999)}",
                        1
                    ))

                    driver_id = cur.lastrowid

                    # Create notifications for driver
                    for _ in range(random.randint(5, 15)):
                        cur.execute("""
                            INSERT INTO Notifications (
                                DriverId, Title, Message, NotificationTime,
                                IsRead, IsDeleted
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            driver_id,
                            fake.sentence(),
                            fake.paragraph(),
                            fake.date_time_this_month(),
                            random.choice([0, 1]),
                            0
                        ))

                user_ids.append((user_id, role_type))

                # Commit in batches
                if i % BATCH_SIZE == 0:
                    conn.commit()

        conn.commit()
        return user_ids
    except Exception as e:
        conn.rollback()
        print(f"Error creating users: {e}")
        raise


def create_packages(conn, num_packages):
    """Create diverse package types"""
    package_types = [
        ('Tiêu chuẩn', '50000', 'Giao hàng tiêu chuẩn 3-5 ngày'),
        ('Nhanh', '100000', 'Giao hàng nhanh 1-2 ngày'),
        ('Hỏa tốc', '200000', 'Giao hàng trong ngày'),
        ('Quốc tế', '500000', 'Giao hàng quốc tế 5-7 ngày'),
        ('Siêu tốc', '300000', 'Giao hàng trong 2h')
    ]

    try:
        with conn.cursor() as cur:
            for i in range(num_packages):
                base_type = random.choice(package_types)
                price_variation = random.randint(-10000, 10000)

                cur.execute("""
                    INSERT INTO Packages (
                        PackageName, Amount, ImageUrl, Description,
                        CreatedAt, UpdatedAt
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    f"{base_type[0]} - {fake.word().capitalize()}",
                    str(int(base_type[1]) + price_variation),
                    f"package_{i + 1}.jpg"[:200],
                    f"{base_type[2]} - {fake.sentence()}",
                    fake.date_time_this_year(),
                    fake.date_time_this_month()
                ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error creating packages: {e}")
        raise


def create_warehouses(conn, num_warehouses):
    """Create warehouses across Vietnam"""
    cities = [
        'Hà Nội', 'Hồ Chí Minh', 'Đà Nẵng', 'Hải Phòng', 'Cần Thơ',
        'Biên Hòa', 'Nha Trang', 'Huế', 'Đà Lạt', 'Vũng Tàu',
        'Quy Nhơn', 'Buôn Ma Thuột', 'Long Xuyên', 'Thái Nguyên',
        'Thanh Hóa', 'Vinh', 'Phan Thiết', 'Hạ Long', 'Mỹ Tho'
    ]

    try:
        with conn.cursor() as cur:
            for i in range(min(num_warehouses, len(cities))):
                city = cities[i]
                cur.execute("""
                    INSERT INTO Warehouses (WarehouseName, Location, Capacity)
                    VALUES (%s, %s, %s)
                """, (
                    f"Kho {city}",
                    f"{fake.street_address()}, {city}",
                    str(random.randint(5000, 20000))
                ))
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        conn.rollback()
        print(f"Error creating warehouses: {e}")
        raise


def create_status_orders(conn):
    """Create order statuses"""
    statuses = [
        ('Pending', 'Đơn hàng mới'),
        ('Confirmed', 'Đã xác nhận'),
        ('PickedUp', 'Đã lấy hàng'),
        ('InTransit', 'Đang vận chuyển'),
        ('Delivered', 'Đã giao hàng'),
        ('Cancelled', 'Đã hủy'),
        ('Failed', 'Giao hàng thất bại')
    ]

    try:
        with conn.cursor() as cur:
            for status, desc in statuses:
                cur.execute("""
                    INSERT INTO StatusOrders (StatusName, CreatedAt, UpdatedAt)
                    VALUES (%s, %s, %s)
                """, (
                    status,
                    datetime.now(),
                    datetime.now()
                ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error creating status orders: {e}")
        raise


def create_orders_batch(conn, user_ids, start_idx, batch_size):
    """Create orders in batches"""
    try:
        with conn.cursor() as cur:
            customer_users = [uid for uid, role in user_ids if role == "User"]

            for i in range(start_idx, start_idx + batch_size):
                order_date = fake.date_time_between(start_date='-1y')
                expected_date = order_date + timedelta(days=3)
                # Luôn set actual_delivery_date
                actual_date = order_date + timedelta(days=random.randint(2, 5))
                user_id = random.choice(customer_users)
                status_id = random.randint(1, 7)
                package_id = str(random.randint(1, NUM_PACKAGES))

                # Create order
                cur.execute("""
                    INSERT INTO Orders (
                        UserId, StatusOrderId, PackageId, CreateOrderDate,
                        PickupLocation, DeliveryLocation, RecipientName,
                        RecipientPhoneNumber, RecipientEmail, RecipientAddress,
                        RecipientCity, RecipientCountry, RecipientPostalCode,
                        ExpectedDeliveryDate, DeliveryDateEstimated, ActualDeliveryDate,
                        Description, TotalAmount, Quantity
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    user_id, status_id, package_id, order_date,
                    fake.address(), fake.address(), fake.name(),
                    fake.phone_number(), fake.email(), fake.street_address(),
                    fake.city(), "Việt Nam", fake.postcode(),
                    expected_date,
                    expected_date,
                    actual_date,  # Luôn có giá trị, không còn None
                    fake.text(max_nb_chars=200),
                    float(random.randint(100000, 1000000)),
                    random.randint(1, 5)
                ))

                order_id = cur.lastrowid

                # Create order packages
                cur.execute("""
                    INSERT INTO OrderPackages (OrderId, PackageId)
                    VALUES (%s, %s)
                """, (order_id, random.randint(1, NUM_PACKAGES)))

                # Create payment
                cur.execute("""
                    INSERT INTO Payments (
                        Amount, PaymentStatus, PaymentMethod,
                        Timestamp, TransactionId, OrderId
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    random.randint(100000, 1000000),
                    PaymentStatus.COMPLETED.value if status_id == 5 else PaymentStatus.PENDING.value,
                    random.choice(list(PaymentMethod)).value,
                    order_date,
                    f"TXN{order_id}",
                    order_id
                ))

                # Create tracking records
                for _ in range(random.randint(2, 5)):
                    cur.execute("""
                        INSERT INTO OrderTrackings (
                            OrderId, CurrentLocation, StatusUpdateTime
                        ) VALUES (%s, %s, %s)
                    """, (
                        order_id,
                        fake.address(),
                        fake.date_time_between(start_date=order_date)
                    ))

            # Create logistics records
            warehouse_id = random.randint(1, NUM_WAREHOUSES)
            package_id = random.randint(1, NUM_PACKAGES)
            quantity = random.randint(10, 100)

            # Inbound logistics
            cur.execute("""
                INSERT INTO InboundLogistics (
                    WarehouseId, PackageId, Quantity, ReceivedDate
                ) VALUES (%s, %s, %s, %s)
            """, (
                warehouse_id,
                package_id,
                quantity,
                fake.date_time_this_year()
            ))

            # Inventory
            cur.execute("""
                INSERT INTO Inventory (
                    WarehouseId, PackageId, Quantity, LastUpdated
                ) VALUES (%s, %s, %s, %s)
            """, (
                warehouse_id,
                package_id,
                quantity,
                fake.date_time_this_month()
            ))

            # Outbound logistics
            out_quantity = random.randint(1, quantity)
            cur.execute("""
                INSERT INTO OutboundLogistics (
                    WarehouseId, PackageId, Quantity, DispatchedDate
                ) VALUES (%s, %s, %s, %s)
            """, (
                warehouse_id,
                package_id,
                out_quantity,
                fake.date_time_this_month()
            ))

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error in batch creation: {e}")
        raise


def create_orders(conn, user_ids, num_orders):
    """Create orders using batch processing"""
    try:
        for start_idx in range(0, num_orders, BATCH_SIZE):
            batch_size = min(BATCH_SIZE, num_orders - start_idx)
            create_orders_batch(conn, user_ids, start_idx, batch_size)
            print(f"Created orders batch: {start_idx + 1} to {start_idx + batch_size}")
    except Exception as e:
        print(f"Error creating orders: {e}")
        raise


def create_logistics_data(conn):
    """Create comprehensive logistics records"""
    try:
        with conn.cursor() as cur:
            # Lấy danh sách warehouse IDs thực tế
            cur.execute("SELECT WarehouseId FROM Warehouses")
            warehouse_ids = [row[0] for row in cur.fetchall()]

            # Lấy danh sách package IDs thực tế
            cur.execute("SELECT PackageId FROM Packages")
            package_ids = [row[0] for row in cur.fetchall()]

            for warehouse_id in warehouse_ids:
                for package_id in package_ids:
                    # Initial stock
                    initial_quantity = random.randint(100, 1000)

                    # Create multiple inbound records
                    for _ in range(random.randint(3, 8)):
                        inbound_qty = random.randint(50, 200)
                        inbound_date = fake.date_time_this_year()

                        cur.execute("""
                            INSERT INTO InboundLogistics (
                                WarehouseId, PackageId, Quantity, ReceivedDate
                            ) VALUES (%s, %s, %s, %s)
                        """, (
                            warehouse_id,
                            package_id,
                            inbound_qty,
                            inbound_date
                        ))

                    # Create outbound records
                    for _ in range(random.randint(5, 15)):
                        outbound_qty = random.randint(10, 50)
                        outbound_date = fake.date_time_this_year()

                        cur.execute("""
                            INSERT INTO OutboundLogistics (
                                WarehouseId, PackageId, Quantity, DispatchedDate
                            ) VALUES (%s, %s, %s, %s)
                        """, (
                            warehouse_id,
                            package_id,
                            outbound_qty,
                            outbound_date
                        ))

                    # Current inventory
                    cur.execute("""
                        INSERT INTO Inventory (
                            WarehouseId, PackageId, Quantity, LastUpdated
                        ) VALUES (%s, %s, %s, %s)
                    """, (
                        warehouse_id,
                        package_id,
                        initial_quantity,
                        datetime.now()
                    ))

                # Commit sau mỗi warehouse để tránh transaction quá lớn
                conn.commit()
                print(f"Created logistics data for warehouse {warehouse_id}")

    except Exception as e:
        conn.rollback()
        print(f"Error creating logistics data: {e}")
        raise


def main():
    """Main execution function"""
    try:
        print("Connecting to database...")
        conn = pymysql.connect(**db_params)

        print("\nStarting data generation process...")

        # print("Creating roles...")
        # role_ids = create_roles(conn)
        # print(f"Created {len(role_ids)} roles")
        #
        # print("\nCreating users...")
        # user_ids = create_users(conn, NUM_USERS, role_ids)
        # print(f"Created {len(user_ids)} users")
        #
        # print("\nCreating status orders...")
        # create_status_orders(conn)
        # print("Status orders created")
        #
        # print("\nCreating packages...")
        # create_packages(conn, NUM_PACKAGES)
        # print(f"Created {NUM_PACKAGES} packages")
        #
        # print("\nCreating warehouses...")
        # create_warehouses(conn, NUM_WAREHOUSES)
        # print(f"Created {NUM_WAREHOUSES} warehouses")
        #
        # print("\nCreating orders...")
        # create_orders(conn, user_ids, NUM_ORDERS)
        # print(f"Created {NUM_ORDERS} orders")

        print("\nCreating logistics data...")
        create_logistics_data(conn)
        print("Logistics data created")

        print("\nData generation completed successfully!")

        # Print summary
        print("\nSummary of generated data:")
        print(f"- Users: {NUM_USERS}")
        print(f"- Orders: {NUM_ORDERS}")
        print(f"- Packages: {NUM_PACKAGES}")
        print(f"- Warehouses: {NUM_WAREHOUSES}")

    except Exception as e:
        print(f"Error in main execution: {e}")
        raise
    finally:
        if 'conn' in globals():
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    if check_database_connection():
        main()
    else:
        print("Failed to connect to database")