from airflow.decorators import dag
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

@dag(schedule=None, start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), catchup=False)
def python_process():
    def print_hello():
        print("Hello from the print_hello")
    
    def print_goodbye():
        print("Good bye")
    
    # create tasks
    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=print_hello
    )
    goodbye_task = PythonOperator(
        task_id="goodbye_task",
        python_callable=print_goodbye
    )

    return [hello_task >> goodbye_task]

simple_dag = python_process()