import mysql.connector

def track_refresh():
    with mysql.connector.connect(
        database="LDTBXH_Stg",
        user="root",
        password="29122003",
        host="host.docker.internal"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO DimAuditForeigned (process_name, start_at, finished_at, information, status)
                VALUES ('data integration', NOW(), NULL, 'refreshed data', 'PENDING')
            """)
        conn.commit()
            
def track_transform():
    with mysql.connector.connect(
        database="LDTBXH_Stg",
        user="root",
        password="29122003",
        host="host.docker.internal"
    ) as conn:
        with conn.cursor() as cur:
            # Lấy giá trị start_at lớn nhất trước và lưu vào biến
            cur.execute("SELECT MAX(start_at) FROM DimAuditForeigned")
            max_start_at = cur.fetchone()[0]

            if max_start_at is not None:
                cur.execute("""
                    UPDATE DimAuditForeigned
                    SET
                        process_name = 'data integration',
                        finished_at = NOW(),
                        information = 'transformed successfully'
                    WHERE start_at = %s
                """, (max_start_at,))
            else:
                print("No records found to update.")
        
        conn.commit()

def track_load():
    with mysql.connector.connect(
        database="LDTBXH_Stg",
        user="root",
        password="29122003",
        host="host.docker.internal"
    ) as conn:
        with conn.cursor() as cur:
            # Lấy giá trị start_at lớn nhất trước và lưu vào biến
            cur.execute("SELECT MAX(start_at) FROM DimAuditForeigned")
            max_start_at = cur.fetchone()[0]

            if max_start_at is not None:
                cur.execute("""
                    UPDATE DimAuditForeigned
                    SET
                        process_name = 'data integration',
                        finished_at = NOW(),
                        information = 'ETL successfully',
                        status = 'SUCCESS'
                    WHERE start_at = %s
                """, (max_start_at,))
            else:
                print("No records found to update.")

        conn.commit()

if __name__ == '__main__':
    print('Tracking operations executed.')
