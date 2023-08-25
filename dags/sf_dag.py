
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
)

with DAG(
    dag_id="snowflake_airflow_demo",
    start_date=datetime(2023, 8, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "conn_id": "snowflake_default",
    },
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

# • Completeness measures how comprehensive your data is.
    """
    #### Completeness
    """
    supplier_completeness = SQLColumnCheckOperator(
        task_id="completeness",
        table="supplier",
        column_mapping={
            "S_SUPPKEY": {"null_check": {"equal_to": 0}},
            "S_NAME": {"null_check": {"equal_to": 0}},
            "S_ACCTBAL": {"null_check": {"equal_to": 0}},
            "S_NATIONKEY": {"null_check": {"equal_to": 0}},
        },
    )

# • Accuracy measures how correct your data is in representing objects within the scope of requirements.
    """
    #### Accuracy
    """
    supplier_accuracy = SQLColumnCheckOperator(
        task_id="accuracy",
        table="supplier",
        column_mapping={
            "S_ACCTBAL": {"min": {"geq_to": -1000}},
            "S_ACCTBAL": {"max": {"leq_to": 10000}},
            "S_NATIONKEY": {"min": {"geq_to": 0}},
            "S_NATIONKEY": {"max": {"leq_to": 50}},
        },
    )


# • Uniqueness looks at duplication within your data.
    """
    #### Uniqueness
    """    
    supplier_uniqueness = SQLColumnCheckOperator(
        task_id="uniqueness",
        table="supplier",
        column_mapping={
            "S_SUPPKEY": {"unique_check": {"equal_to": 0}},
        },
    )

# • Timeliness ensures that your data is fresh and available within your requirements/SLAs.
    """
    Timeliness
    """
    supplier_timeliness = SQLTableCheckOperator(
        task_id="timeliness",
        table="supplier",
        checks={"row_count_check": {"check_statement": "COUNT(*) = 10000"}},
    )

start  >> supplier_completeness >> supplier_accuracy >> supplier_uniqueness >> supplier_timeliness >> end