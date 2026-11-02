from airflow.sdk import dag, task, task_group

@dag
def group():

    @task
    def a():
        return 42

    @task_group(
            # Will try twice in this task group before failing.
            default_args={
                "retries": 2
            }
    )
    def my_group(val: int):

        @task
        def b(my_val: int):
            print(my_val + 42)

        @task_group(
                default_args={
                    "retries": 3
                }
        )
        def my_nested_task_group():

            @task
            def c():
                print("c")

            c() 

        b(val) >> my_nested_task_group()

    # a() >> b() >> c()
    # When running task group. Substitute it for the individual tasks.
    # Then, if you want to add values, you can do what's below.
    val = a() 
    my_group(val)

group()   