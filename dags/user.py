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

# This materializes multiple assets at the same time.

@asset.multi(
    schedule= user,
    outlets= [
        Asset(name="user_location"),
        Asset(name="user_login")
    ]   
)
def user_info(user: Asset, context: Context) -> list[dict[str]]:
    user_data = context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids = user.name,
        include_prior_dates = True,
    ) 
    return [
        user_data['results'][0]['location'],
        user_data['results'][0]['login']
    ]
