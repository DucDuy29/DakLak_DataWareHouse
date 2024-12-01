import requests
import mysql.connector
from mysql.connector import Error

# Kết nối đến cơ sở dữ liệu MySQL
def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        print("Kết nối đến MySQL thành công")
    except Error as e:
        print(f"Error: '{e}'")
    return connection

# Lấy dữ liệu từ API
def fetch_data_from_api(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Kiểm tra lỗi
        data = response.json()  # Trả về dữ liệu JSON
        print("Dữ liệu nhận được từ API:", data)  # In ra dữ liệu để kiểm tra cấu trúc
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# Chèn dữ liệu vào bảng thông qua stored procedure
def insert_data_to_db(connection, data, stored_procedure_name):
    if data is not None:
        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, list):
                    try:
                        values = tuple(entry)
                        cursor = connection.cursor()
                        # Gọi stored procedure
                        cursor.callproc(stored_procedure_name, values)
                        connection.commit()
                        print(f"Dữ liệu đã được chèn thành công: {values}")  # Thông báo thành công
                        cursor.close()
                    except Error as e:
                        print(f"Error inserting data: {e}")
                        connection.rollback()  # Hoàn tác nếu có lỗi
                else:
                    print("Mục không phải là danh sách:", entry)
        else:
            print("Dữ liệu không phải là danh sách.")
    else:
        print("Không có dữ liệu để chèn.")


# Chạy chương trình
def run_load_api_to_stgBaoCaoNCC_Fact():
    # Đường dẫn API và thông tin kết nối DB
    api_url_Fact = "http://host.docker.internal:8000/BaoCaoNCC"  # Đường dẫn API cho bảng
    host = "host.docker.internal"
    user = "root"
    password = "29122003"
    database = "LDTBXH_Stg"  # Thay bằng tên cơ sở dữ liệu thực tế

    # Kết nối đến cơ sở dữ liệu
    db_connection = create_connection(host, user, password, database)

    # Lấy dữ liệu từ API
    api_data = fetch_data_from_api(api_url_Fact)

    # Chèn dữ liệu vào bảng tương ứng
    insert_data_to_db(db_connection, api_data, 'insert_Stg_BaoCaoNCC_Fact')
