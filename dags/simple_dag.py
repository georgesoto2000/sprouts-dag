from datetime import datetime
from airflow.decorators import dag, task


@dag(
    dag_id="test_dag_visibility",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def test_dag():
    @task
    def hello():
        print("Hello Airflow!")

    hello()


# No function call
