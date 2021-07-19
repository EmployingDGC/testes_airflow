from random import randint

from airflow import DAG
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator
)
from airflow.operators.bash import BashOperator

from datetime import datetime


def _training_model():
    return randint(1, 10)


def _choose_best_model(ti):
    accuracies = ti.xcom_pull(
        task_ids=[
            "training_model_a",
            "training_model_b",
            "training_model_c"
        ]
    )

    best_accuracy = max(accuracies)

    if best_accuracy > 8:
        return 'accurate'

    return 'inaccurate'


if __name__ == "__main__":
    with DAG(
        dag_id="my_dag",
        start_date=datetime(
            day=16,
            month=2,
            year=2022
        ),
        schedule_interval="@daily",
        catchup=False
    ) as dag:
        training_model_a = PythonOperator(
            task_id="training_model_a",
            python_callable=_training_model
        )

        training_model_b = PythonOperator(
            task_id="training_model_b",
            python_callable=_training_model
        )

        training_model_c = PythonOperator(
            task_id="training_model_c",
            python_callable=_training_model
        )

        choose_best_model = BranchPythonOperator(
            task_id="choose_best_model",
            python_callable=_choose_best_model
        )

        accurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'accurate'"
        )

        inaccurate = BashOperator(
            task_id="inaccurate",
            bash_command="echo 'inaccurate'"
        )

        [
            training_model_a,
            training_model_b,
            training_model_c
        ] >> choose_best_model >> [
            accurate,
            inaccurate
        ]
