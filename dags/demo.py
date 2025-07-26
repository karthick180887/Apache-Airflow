from airflow.models import DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    #task1
    create_pet_table = SQLExecuteQueryOperator(
        task_id="create_pet_table",
        conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
    )
    #task2
    populate_pet_table = SQLExecuteQueryOperator(
        task_id="populate_pet_table",
        conn_id="postgres",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )
    #task3
    get_all_pets = SQLExecuteQueryOperator(task_id="get_all_pets", conn_id="postgres", sql="SELECT * FROM pet;")
    #task4
    get_birth_date = SQLExecuteQueryOperator(
        task_id="get_birth_date",
        conn_id="postgres",
        sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        hook_params={"options": "-c statement_timeout=3000ms"},
    )
    
    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date