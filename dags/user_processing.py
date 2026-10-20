from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue

# This creates a task in postgres.
@dag
def user_processing():

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

# This will check the API condition every 30 seconds.
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

# Uses task decorator. Exact same as using PythonOperator.
    @task
    def extract_user():
        #  ti = task instance that can take data from another/previous task. ti is a reserved keyword.
        # fake_user = ti.xcom_pull(task_ids="is_api_available")

        import requests
        response = requests.get('https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json')
        fake_user = response.json()

        return {
            'id': fake_user['id'],
            'first_name': fake_user['personalInfo']['firstname'],
            'last_name': fake_user['personalInfo']['lastname'],
            'email': fake_user['personalInfo']['email']
        }


    is_api_available()

user_processing()

