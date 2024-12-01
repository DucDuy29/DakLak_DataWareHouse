import pandas as pd
import mysql.connector

def read_date_excel():
    # Read data from Excel
    time_df = pd.read_excel('/opt/airflow/data/SampleDateDim.xls', sheet_name='LoadDates')

    # Connect to MySQL
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="root",
        password="29122003",
        database="LDTBXH_Stg"
    )
    cur = conn.cursor()
    
    # Insert data into MySQL
    print('Starting insertion...')
    try:
        cols = ','.join(list(time_df.columns))
        
        for row in time_df.to_numpy():
            # Convert any Timestamp types to string for SQL insertion
            row = [str(val) if isinstance(val, pd.Timestamp) else val for val in row]
            
            # Format values for MySQL insertion
            cur.execute(f'''INSERT INTO Stg_Date ({cols}) VALUES ({', '.join(['%s'] * len(row))})''', row)
        
        print('Inserted successfully!')
    except Exception as exc:
        print("Error:", str(exc))
    
    conn.commit()
    cur.close()
    conn.close()
    
if __name__ == '__main__':
    read_date_excel()

