from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

VENV_PYTHON ="/home/swathi/airflow/airflow_env/bin/python"


default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='book_etl_pipeline',
    default_args=default_args,
    schedule_interval='16 21 * * *',  # You can change this later to '@daily'
    catchup=False,
    tags=["etl", "books"]
) as dag:

    extract = BashOperator(
        task_id='extract_books',
        bash_command=f'{VENV_PYTHON} /home/swathi/airflow/etl/extract/fetch_book_data.py'
    )

    clean = BashOperator(
        task_id='clean_books',
        bash_command=f'{VENV_PYTHON} /home/swathi/airflow/etl/transform/clean_books_data.py'
    )

    load = BashOperator(
        task_id='load_to_db',
        bash_command=f'{VENV_PYTHON} /home/swathi/airflow/etl/load/load_to_db.py'
    )

    extract >> clean >> load
