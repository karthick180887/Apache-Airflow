from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator
import pendulum
@dag(schedule=None, start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), catchup=False)
def simple_bash_dag():
    "Simple Dag"
    #Task1
    print_date = BashOperator(
        task_id="print_date",
        bash_command="date"
    )
    #task2
    list_files = BashOperator(
        task_id="list_files",
        bash_command="ls -l"
    )
    
    #Set Dependencies
    return [print_date>> list_files]

bash_dag = simple_bash_dag()