from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner': 'dhanasekar',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
    dag_id='first_dag_v5',
    default_args=default_args,
    description='This is our first dag that we',
    start_date=datetime(2023, 3, 24, 0, 5, 0),
    schedule_interval="@daily"
) as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command='echo hello world, this is the first task!'
    )
    task2=BashOperator(
        task_id='second_task',
        bash_command='echo hey, I am task 2 and will be executed after running task 1'
    )
    task3=BashOperator(
        task_id='third_task',
        bash_command='echo hey, I am task 3 and will be executed after running task 2'
    )

    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    task1 >> task2
    task1 >> task3

    # Task dependency method 3
    task1 >> [task2, task3]
