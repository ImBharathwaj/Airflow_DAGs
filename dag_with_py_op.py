from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'me',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}


def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello, World! My name is {first_name} {last_name} and \
    I'm {age} years old!")


def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Friedman')


def get_age(ti):
    ti.xcom_push(key='age', value=20)


with DAG(
        dag_id='First DAG with python operator',
        description='DAG with Python Operator',
        default_args=default_args,
        start_date=datetime(2023, 5, 1),
        schedule_interval='@daily'
) as dag:
    task1: PythonOperator = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task2: PythonOperator = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3: PythonOperator = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task2, task3] >> task2
