from airflow.sdk import dag, task, Context

@dag
def xcom_dag():

    # 2 methods to push value 42.
    # xcom_push(), import Context class
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

    t1() >> t2()

xcom_dag()