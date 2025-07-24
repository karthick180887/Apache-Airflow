from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum
@dag(schedule=None, start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), catchup=False)
def create_table():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS karthick (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
create_table()