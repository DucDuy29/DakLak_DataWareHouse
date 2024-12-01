import pandas as pd
import mysql.connector

def calculate_scores(df):
    # Calculate TongDiemB1B2 as the sum of DiemB1 and Diem_B2
    df['TongDiemB1B2'] = df['DiemB1'].fillna(0) + df['Diem_B2'].fillna(0)  # Fill NaN with 0 for calculation
    
    # Determine KetQuaCuoiCung based on TongDiemB1B2
    conditions = [
        (df['TongDiemB1B2'] >= 150),
        (df['TongDiemB1B2'] >= 100) & (df['TongDiemB1B2'] < 150)
    ]
    choices = ['Hộ Nghèo', 'Hộ Cận Nghèo']
    df['KetQuaCuoiCung'] = 'Hộ Không Nghèo'  # Default
    df['KetQuaCuoiCung'] = df['KetQuaCuoiCung'].where(~conditions[0], choices[0])
    df['KetQuaCuoiCung'] = df['KetQuaCuoiCung'].where(~conditions[1], choices[1])
    
    return df

def run_transform_fact_hongheo():  
    # Connect to MySQL
    conn = mysql.connector.connect(
        host='host.docker.internal',
        user='root',
        password='29122003',
        database='LDTBXH_Stg'
    )
    conn1 = mysql.connector.connect(
        host='host.docker.internal',
        user='root',
        password='29122003',
        database='db_hongheo_daknong'
    )
    
    # Load data from MySQL tables
    query = '''
        SELECT 
            h.ID_Ho as ID_HoGiaDinh, h.HoTenChuHo, j.LoaiKhuVuc, j.TenKhuVuc, max(m.NgayRaSoat) as NgayRaSoat, k.DiemB1, l.Diem_B2
        FROM 
            db_hongheo_daknong.HoGiaDinh h
        JOIN 
            db_hongheo_daknong.KhuVuc j ON h.ID_KhuVuc = j.ID_KhuVuc
        JOIN 
            db_hongheo_daknong.PhieuB1 k ON h.ID_Ho = k.ID_Ho
        JOIN 
            db_hongheo_daknong.PhieuB2 l ON h.ID_Ho = l.ID_Ho
        JOIN 
            db_hongheo_daknong.RaSoat m ON h.ID_Ho = m.ID_Ho
        GROUP BY 
            h.ID_Ho, h.HoTenChuHo, j.LoaiKhuVuc, j.TenKhuVuc, k.DiemB1, l.Diem_B2;
        '''
    data_df = pd.read_sql(query, conn1)
        
    # Perform calculations for TongDiemB1B2 and KetQuaCuoiCung
    result_df = calculate_scores(data_df)
    
    # Drop rows where ID_HoGiaDinh or KetQuaCuoiCung is NaN
    result_df.dropna(subset=['ID_HoGiaDinh', 'KetQuaCuoiCung'], inplace=True)

    # Update existing rows in the MySQL table
    cursor = conn.cursor()
    for _, row in result_df.iterrows():
        update_query = '''
            UPDATE Stg_HoGDRaSoat_Fact
            SET
                TongDiemB1B2 = %s,
                KetQuaCuoiCung = %s
            WHERE ID_HoGiaDinh = %s
        '''
        cursor.execute(update_query, (row['TongDiemB1B2'], row['KetQuaCuoiCung'], row['ID_HoGiaDinh']))
    
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_transform_fact_hongheo()
