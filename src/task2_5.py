import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import cross_downstream

with DAG(
    dag_id="Task_5_DAG",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    task1 = DummyOperator(task_id="task1")
    task2 = DummyOperator(task_id="task2")
    task3 = DummyOperator(task_id="task3")
    task4 = DummyOperator(task_id="task4")
    task5 = DummyOperator(task_id="task5")
    task6 = DummyOperator(task_id="task6")

    task1 >> [task2, task3]
    cross_downstream([task2, task3], [task4, task5, task6])