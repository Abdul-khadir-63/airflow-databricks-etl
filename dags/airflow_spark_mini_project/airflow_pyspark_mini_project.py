from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context


from utils.databricks_validation import *


@dag(
    dag_id="airflow_pyspark_mini_project",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["production", "databricks"],
)
def airflow_pyspark_pipeline():

    # ---------------------------------------------------
    # Bronze
    # ---------------------------------------------------

    run_bronze = DatabricksRunNowOperator(
        task_id="run_bronze",
        databricks_conn_id="databricks_default",
        job_id=1089085716505123,
    )

    # ---------------------------------------------------
    # Silver
    # ---------------------------------------------------

    run_silver = DatabricksRunNowOperator(
        task_id="run_silver",
        databricks_conn_id="databricks_default",
        job_id=78935676834460,
    )

    # ---------------------------------------------------
    # Validation
    # ---------------------------------------------------

    run_validation = DatabricksRunNowOperator(
        task_id="run_validation",
        databricks_conn_id="databricks_default",
        job_id=144199432296402,
        notebook_params={
            "pipeline_run_id": "{{ run_id }}"
        },
            
    )

    # ---------------------------------------------------
    # Read Validation Result
    # ---------------------------------------------------

    @task
    def validation_gate():
        context = get_current_context()
        current_run_id = context["run_id"]
        conn = BaseHook.get_connection("databricks_sql")

        return get_validation_result(
            server_hostname=conn.host,
            http_path=conn.extra_dejson["http_path"],
            access_token=conn.password,
            pipeline_run_id=current_run_id,
        )


    # ---------------------------------------------------
    # Branch
    # ---------------------------------------------------

    @task.branch
    def choose_path(result: str):

        if result == "PASS":
            return "run_gold"

        return "notify_failure"

    # ---------------------------------------------------
    # Gold
    # ---------------------------------------------------

    run_gold = DatabricksRunNowOperator(
        task_id="run_gold",
        databricks_conn_id="databricks_default",
        job_id=197697026403419,
    )

    # ---------------------------------------------------
    # Notification
    # ---------------------------------------------------

    @task
    def notify_failure():

        print("Validation Failed.")
        print("Gold Layer Skipped.")

    # ---------------------------------------------------
    # TaskFlow
    # ---------------------------------------------------

    validation_status = validation_gate()
    

    decision = choose_path(validation_status)

    (
        run_bronze
        >> run_silver
        >> run_validation
        >> validation_status
        >> decision
    )

    decision >> run_gold
    decision >> notify_failure()


airflow_pyspark_pipeline()