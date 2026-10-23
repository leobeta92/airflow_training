from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook 

# An operator describes a single task in a workflow.

# This is the dag decorator used to define a dag.
@dag
def user_processing():
    
    # This operator runs a SQL query with postgres after creating a connection with the service. These operators exist for other services like GCP.
    create_table = SQLExecuteQueryOperator(
        task_id = 'create_table',
        conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
"""
    )

# A sensor is a long-running task waiting for an event to happen. A poke function is called every n seconds to check if the criteria are met.

# This will check the API condition every 30 seconds. Waits for it to be true.
# This task is a sensor which uses a task decorator.
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests
        response = requests.get('https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json')
        print(response.status_code)
        if response.status_code == 200:
              condition = True
              fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

# Uses task decorator. Exact same as using PythonOperator. Recommended when we want to run Python code.
    @task
    def extract_user(fake_user):

        return {
            'id': fake_user['id'],
            'first_name': fake_user['personalInfo']['firstName'],
            'last_name': fake_user['personalInfo']['lastName'],
            'email': fake_user['personalInfo']['email']
        }

    @task
    def process_user(user_info):

        import csv
        from datetime import datetime as dt

        user_info['created_at'] = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        with open('/tmp/user_info.csv', 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info) 

    # A hook is used when we want to run methods that are not included in the operator. 
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id='postgres')
        hook.copy_expert(
            sql = "COPY users FROM STDIN WITH CSV HEADER",
            filename='/tmp/user_info.csv'
        )

# In order to put the tasks in order, need a Bitshift operator.
    """
    task a >> task b will run in order.
    or
    task a >> task b 
    task b >> task c will run them just like above, but they're just on separate lines.
    or
    task a >> [task b, task c, ...] >> task e will run at the same time, then task e.
    """

    process_user(extract_user(create_table >> is_api_available())) >> store_user()

user_processing()
