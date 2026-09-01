from airflow import DAG 
from airflow.operators.bash import BashOperator 
from datetime import datetime, timedelta 

default_args = {
    'owner': 'harish', 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_volatility_pipeline', 
    default_args=default_args, 
    description='Fetches Bitcoin data every 5 mins', 
    start_date = datetime(2026, 1, 26), 
    schedule_interval='*/5 * * * *',
    catchup=False,
) as dag: 

    extract_bitcoin_task = BashOperator(
        task_id='extract_bitcoin_data',
        bash_command='python /opt/airflow/dags/extract_data.py'
    )

    extract_bitcoin_task