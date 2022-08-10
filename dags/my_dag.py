from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        res = 'accurate'
    else:
        res = 'inaccurate'
    with open("output.log", "w") as f:
        f.write(res)
    return res


def _training_model():
    return randint(1, 10)

with DAG("my_dag", start_date=datetime(2021, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

        training_model_A = PythonOperator(
            task_id="training_model_A",
            python_callable=_training_model
        )

        training_model_B = PythonOperator(
            task_id="training_model_B",
            python_callable=_training_model
        )

        training_model_C = PythonOperator(
            task_id="training_model_C",
            python_callable=_training_model
        )

        choose_best_model = BranchPythonOperator(
            task_id="choose_best_model",
            python_callable=_choose_best_model
        )

        accurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'accurate' | tee out.log"
        )

        inaccurate = BashOperator(
            task_id="inaccurate",
            bash_command="echo 'inaccurate' | tee out.log"
        )
        
        pwd = BashOperator(
            task_id="pwd",
            bash_command="pwd"
        )
        
        file1 = BashOperator(
            task_id="file1",
            bash_command="touch file1.txt"
        )
        
        file2 = BashOperator(
            task_id="file2",
            bash_command="touch file2.txt"
        )
        
        sleep = BashOperator(
            task_id="sleep",
            bash_command="sleep 60"
        )

        [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]
        pwd >> ls >> [file1, file2] >> sleep
