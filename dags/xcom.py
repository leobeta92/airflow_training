from airflow.sdk import dag, task, Context
from typing import Dict, Any

@dag
def xcom_dag():

    # 2 methods to push value 42.
    # xcom_push(), import Context class

    """
        @task
        def t1(context: Context):
            val = 42
            context['task_instance'].xcom_push(
                key='my_key',
                value=val
            )

        @task
        def t2(context: Context):
            val = context['ti'].xcom_pull(
                task_ids='t1',
                key='my_key'
            )

            print(val)
    """

# Method 2
    @task
    # def t1() -> int:
        # my_val = 42
        # return val # xcom_push(key and value)

    def t1() -> Dict[str, Any]:
        my_val = 42
        my_sentence = 'Hello, World!'

        return {
            "my_val": my_val,
            "my_sentence": my_sentence
        }

    @task
    # def t2(val: int):
    #     print(val)

    def t2(data: Dict[str, Any]):
        print(data['my_val'])
        print(data['my_sentence'])

    val = t1() 
    t2(val)

xcom_dag()