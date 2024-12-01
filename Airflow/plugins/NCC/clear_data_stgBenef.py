import pandas as pd
import mysql.connector
import re

def clean_and_update_Stg_DimBenef():
    # Thông tin kết nối tới MySQL
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="root",
        password="29122003",
        database="LDTBXH_Stg"
    )
    
    # Đọc dữ liệu từ bảng trong MySQL
    query = "SELECT * FROM Stg_DimBenef"
    df = pd.read_sql(query, conn)
    
    # Hàm xử lý cột dateOfBirth
    def parse_date(value):
        if isinstance(value, str):
            # Xử lý định dạng '1929-12-31T17:00:00.000Z'
            value = re.sub(r'T.*Z$', '', value)  # Loại bỏ phần 'T...Z'
            
            if re.match(r'^\d{4}-\d{2}-\d{2}$', value):  # Định dạng YYYY-MM-DD
                parsed_date = pd.to_datetime(value, errors='coerce')
                if pd.isna(parsed_date):
                    return 'N/A'  # Trả về 'N/A' nếu ngày không hợp lệ
                return parsed_date.strftime('%Y-%m-%d')
            elif re.match(r'^\d{4}-\d{2}$', value):  # Định dạng YYYY-MM
                return value  # Giữ nguyên dạng YYYY-MM
            elif re.match(r'^\d{4}$', value):  # Chỉ có năm
                return value  # Giữ nguyên dạng YYYY
        return 'N/A'  # Nếu không hợp lệ hoặc NULL
    
    # Áp dụng hàm parse_date vào cột dateOfBirth
    df['dateOfBirth'] = df['dateOfBirth'].apply(lambda x: parse_date(x) if pd.notnull(x) else 'N/A')
    
    # Thay thế tất cả các giá trị NULL còn lại bằng 'N/A'
    df = df.fillna('N/A')
    
    
    # Khởi tạo cursor để cập nhật dữ liệu
    cursor = conn.cursor()

    # Cập nhật từng bản ghi vào bảng MySQL
    for index, row in df.iterrows():
        update_values = []
        update_columns = []

        # Kiểm tra và thêm giá trị cho từng cột (bỏ qua lỗi nếu có)
        try:
            update_columns.append('fullName_Benef')
            update_values.append(row['fullName_Benef'])
        except Exception as e:
            print(f"Error processing fullName_Benef for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('dateOfBirth')
            update_values.append(row['dateOfBirth'])
        except Exception as e:
            print(f"Error processing dateOfBirth for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('gender')
            update_values.append(row['gender'])
        except Exception as e:
            print(f"Error processing gender for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('identityCard_Benef')
            update_values.append(row['identityCard_Benef'])
        except Exception as e:
            print(f"Error processing dateOfBirth for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('nativePlace_Benef')
            update_values.append(row['nativePlace_Benef'])
        except Exception as e:
            print(f"Error processing nativePlace_Benef for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('relation_NCC')
            update_values.append(row['relation_NCC'])
        except Exception as e:
            print(f"Error processing relation_NCC for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('beneficiary')
            update_values.append(row['beneficiary'])
        except Exception as e:
            print(f"Error processing beneficiary for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('IsNCC')
            update_values.append(row['IsNCC'])
        except Exception as e:
            print(f"Error processing IsNCC for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('status')
            update_values.append(row['status'])
        except Exception as e:
            print(f"Error processing status for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('province_NCC')
            update_values.append(row['province_NCC'])
        except Exception as e:
            print(f"Error processing province_NCC for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('district_NCC')
            update_values.append(row['district_NCC'])
        except Exception as e:
            print(f"Error processing district_NCC for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('ward_NCC')
            update_values.append(row['ward_NCC'])
        except Exception as e:
            print(f"Error processing ward_NCC for Id_Benef {row['Id_Benef']}: {e}")
        
        try:
            update_columns.append('createdAt')
            update_values.append(row['createdAt'])
        except Exception as e:
            print(f"Error processing createdAt for Id_Benef {row['Id_Benef']}: {e}")
        
        # Cập nhật giá trị vào bảng nếu có cột nào hợp lệ
        if update_values:
            try:
                cursor.execute(f"""
                    UPDATE Stg_DimBenef
                    SET 
                        {', '.join([f"{col} = %s" for col in update_columns])}
                    WHERE Id_Benef = %s
                """, tuple(update_values) + (row['Id_Benef'],))
            except mysql.connector.Error as e:
                print(f"Error updating row with Id_Benef {row['Id_Benef']}: {e}")

    # Xác nhận thay đổi và đóng kết nối
    conn.commit()
    cursor.close()
    conn.close()

# Gọi hàm để thực hiện quá trình xử lý và cập nhật
if __name__ == '__main__':
    clean_and_update_Stg_DimBenef()
