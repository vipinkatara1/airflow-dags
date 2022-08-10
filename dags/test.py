import sys
import importlib	
import subprocess
def reqiuiredModule(lib):
    try:
        importlib.import_module(lib)
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install',lib])
reqiuiredModule("pandas")

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def read_csv():
    df = pd.read_csv("/opt/airflow/dags/repo/data/sparse-cuda.csv")
    print(df)

def write_csv():
    pass

with DAG("test_dag", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    
    read = PythonOperator(
        task_id="read_csv",
        python_callable=read_csv
    )

    write = PythonOperator(
        task_id="write_csv",
        python_callable=write_csv
    )

    read >> write