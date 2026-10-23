from airflow.sdk import asset, Asset, Context

@asset(
    # function with name defined below. But you could put:
    # name = "user"
    schedule= "@daily",
    # Should always try to define one.
    uri = "https://randomuser.me/api"
)

# self can access properties defined in asset decorator.
# Adding typing to function.
def user(self) -> dict[str]:
    import requests

    r = requests.get(self.uri)
    return r.json()

@asset(
    schedule = user
)
def user_location(user: Asset, context: Context) -> dict[str]:
    user_data = context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids = user.name,
        include_prior_dates = True,
    )
    return user_data['results'][0]['location']