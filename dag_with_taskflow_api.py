from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'me',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


@dag(
    dag_id='dag_with_taskflow_api',
    default_args=default_args,
    start_date=datetime(2023, 5, 1),
    schedule_interval='@daily'
)
def helloworld():
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Jerry',
            'last_name': 'Friedman'
        }

    @task()
    def get_age():
        return 18

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello, World! My name is {first_name} {last_name} and \
        I am {age} years old")

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], last_name=name_dict['last_name'], age=age)


greet_dag = helloworld()
