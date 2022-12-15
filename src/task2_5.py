from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    "etl_dummy_daily",
    start_date=days_ago(1),
    schedule_interval=None,
) as dag:
    
    task1 = DummyOperator(task_id="task1")
    task2 = DummyOperator(task_id="task2")
    task3 = DummyOperator(task_id="task3")
    task4 = DummyOperator(task_id="task4")
    task5 = DummyOperator(task_id="task5")
    task6 = DummyOperator(task_id="task6")
    
    task1 >> [task2, task3]
    task2 >> [task4, task5, task6]
    task3 >> [task4, task5, task6]