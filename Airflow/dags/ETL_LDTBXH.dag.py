from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime

from HoNgheo.Transform import run_transform_fact_hongheo    #type: ignore
from Date.LoadDate import read_date_excel   #type: ignore
from NCC.load_api_to_stgNCC import run_load_api_to_stgNCC   #type: ignore
from NCC.load_api_to_stgBenef import run_load_api_to_stgBenef   #type: ignore
from NCC.load_api_to_stgBaoCaoNCC_Fact import run_load_api_to_stgBaoCaoNCC_Fact   #type: ignore
from NCC.clear_data_stgNCC import clean_and_update_Stg_DimNCC   #type: ignore
from NCC.clear_data_stgBenef import clean_and_update_Stg_DimBenef   #type: ignore
from NCC.clear_data_stgFact import clean_and_update_Stg_BaoCaoNCC_Fact   #type: ignore
from CheckAudit.Check_Audit import track_transform, track_load, track_refresh   #type: ignore


def start():
    print('Starting ETL process to DATA WAREHOUSE...')

def end():
    print('Finish at {}!'.format(datetime.today().date()))
    
def excecute_load_api_stgFact(file_path_Fact):
    with open(file_path_Fact, 'r') as file:
        exec(file.read())

def excecute_load_api_stgDimNCC(file_path_DimNCC):
    with open(file_path_DimNCC, 'r') as file:
        exec(file.read())

def excecute_load_api_stgDimBenef(file_path_DimBenef):
    with open(file_path_DimBenef, 'r') as file:
        exec(file.read())

with DAG(
    'ETL_To_Data_Warehouse',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
) as dag:
    #Start
    start_task = PythonOperator(
        task_id = 'start',
        python_callable=start,
        dag=dag
    )
    
    truncate = SQLExecuteQueryOperator(
        task_id='truncate',
        conn_id='ldtbxh_stage',
        sql='./sql_query/truncate_stg.sql',
        dag=dag
    )
    
    #Ho Ngheo
    load_data_stage_hongheo = SQLExecuteQueryOperator(
        task_id= 'load_data_stage_hongheo',
        conn_id='ldtbxh_stage',
        sql='./sql_query/load_hongheo_stg.sql',
        dag=dag
    )
    
    update_measure_stage_hongheo = PythonOperator(
        task_id='update_measure_stage_hongheo',
        python_callable=run_transform_fact_hongheo,
        dag=dag
    )
    
    load_data_dwh_hongheo = SQLExecuteQueryOperator(
        task_id='load_data_dwh_hongheo',
        conn_id='ldtbxh_dwh',
        sql='./sql_query/load_hongheo_dwh.sql',
        dag=dag
    )
    
    #NCC
    load_data_to_stgNCC = PythonOperator(
        task_id='load_data_to_stgNCC',
        python_callable=run_load_api_to_stgNCC,
        dag=dag
    )

    
    load_data_to_stgBenef = PythonOperator(
        task_id = 'load_data_to_stgBenef',
        python_callable=run_load_api_to_stgBenef,
        dag=dag
    )
    
    load_data_to_stgFact = PythonOperator(
        task_id = 'load_data_to_stgFact',
        python_callable=run_load_api_to_stgBaoCaoNCC_Fact,
        dag=dag
    )
    
    clear_data_to_stgNCC = PythonOperator(
        task_id = 'clear_data_to_stgNCC',
        python_callable=clean_and_update_Stg_DimNCC,
        dag=dag
    )
    
    clear_data_to_stgBenef = PythonOperator(
        task_id = 'clear_data_to_stgBenef',
        python_callable=clean_and_update_Stg_DimBenef,
        dag=dag
    )
    
    clear_data_to_stgFact = PythonOperator(
        task_id = 'clear_data_to_stgFact',
        python_callable=clean_and_update_Stg_BaoCaoNCC_Fact,
        dag=dag
    )
    
    load_data_dwh_ncc = SQLExecuteQueryOperator(
        task_id='load_data_dwh_ncc',
        conn_id='ldtbxh_dwh',
        sql='./sql_query/load_ncc_dwh.sql',
        dag=dag
    )
    
    #Check Audit
    audit_transform = PythonOperator(
        task_id='audit_transform',
        python_callable=track_transform,
        dag=dag
    )
    
    audit_load = PythonOperator(
        task_id = 'audit_load',
        python_callable=track_load,
        dag=dag
    )
    
    #DimDate
    load_date_stage = PythonOperator(
        task_id='load_date_stage',
        python_callable=read_date_excel,
        dag=dag
    )
    
    load_date_dwh = SQLExecuteQueryOperator(
        task_id='load_date_dwh',
        conn_id='ldtbxh_dwh',
        sql='./sql_query/load_date_dwh.sql',
        dag=dag
    )
    #End
    end_task = PythonOperator(
        task_id = 'end',
        python_callable=end,
        dag=dag
    )
    
    start_task >> truncate 
    
    truncate >> load_data_stage_hongheo >> update_measure_stage_hongheo >> audit_transform 
    truncate >> load_data_to_stgNCC >> clear_data_to_stgNCC >> audit_transform
    truncate >> load_data_to_stgBenef >> clear_data_to_stgBenef >> audit_transform
    truncate >> load_data_to_stgFact >> clear_data_to_stgFact >> audit_transform
    
    truncate >> load_date_stage >> load_date_dwh >> end_task
    
    audit_transform >> load_data_dwh_hongheo >> audit_load >> end_task
    audit_transform >> load_data_dwh_ncc >> audit_load >> end_task
    
    
    