from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

import pandas as pd
import snowflake.connector
from datetime import datetime
import os

from adlfs import AzureBlobFileSystem



# Configuration des chemins Blob
BLOB_STORAGE_CONTAINER = os.getenv("AZURE_BLOB_CONTAINER")
BLOB_STORAGE_ACCOUNT = os.getenv("AZURE_BLOB_ACCOUNT")
BLOB_STORAGE_SAS_TOKEN = os.getenv("AZURE_BLOB_SAS")
TEST_BLOB_PATH = "ERP/students.csv"  # fichier test

BLOB_PATHS = {
    "students": "ERP/students.csv",
    "enrollments": "ERP/enrollments.csv",
    "courses": "ERP/courses.csv",
    "contacts": "CRM/contacts.csv",
    "interactions": "CRM/interactions.csv",
    "leads": "CRM/leads.csv"
}

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": "WH_EDU",
    "database": "EDU_DB",
    "schema": "BRONZE"
}

# Vérification Azure
def check_azure_connection():
    fs = AzureBlobFileSystem(account_name=BLOB_STORAGE_ACCOUNT, sas_token=BLOB_STORAGE_SAS_TOKEN)
    with fs.open(f"{BLOB_STORAGE_CONTAINER}/{TEST_BLOB_PATH}", mode='rb') as f:
        print("✅ Connexion Azure OK — fichier lisible.")

# Vérification Snowflake
def check_snowflake_connection():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_USER(), CURRENT_VERSION()")
    result = cursor.fetchone()
    print(f"✅ Connexion Snowflake OK — utilisateur : {result[0]}, version : {result[1]}")
    cursor.close()
    conn.close()


def load_csv_to_snowflake(table_name, blob_path, **kwargs):
    container = BLOB_STORAGE_CONTAINER
    account = BLOB_STORAGE_ACCOUNT
    sas = BLOB_STORAGE_SAS_TOKEN

    # Téléchargement depuis Azure Blob
    fs = AzureBlobFileSystem(account_name=account, sas_token=sas)
    blob_full_path = f"{container}/{blob_path}"
    local_file = f"/tmp/{table_name}.csv"

    with fs.open(blob_full_path, mode="rb") as f:
        df = pd.read_csv(f)
        df.to_csv(local_file, index=False)

    # Connexion à Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="WH_EDU",
        database="EDU_DB",
        schema="BRONZE"
    )
    cursor = conn.cursor()

    # Création de la table si elle n'existe pas
    cols = ", ".join([f'"{col}" TEXT' for col in df.columns])
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS EDU_DB.BRONZE.raw_{table_name} ({cols})
        """)

    # Truncation de la table
    cursor.execute(f"""
    TRUNCATE TABLE EDU_DB.BRONZE.raw_{table_name}
        """)

    # Envoi vers le stage temporaire
    cursor.execute(f"PUT file://{local_file} @edu_stage OVERWRITE = TRUE")

    # COPY INTO depuis le stage
    cursor.execute(f"""
        COPY INTO EDU_DB.BRONZE.raw_{table_name}
        FROM @edu_stage/{table_name}.csv
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE'
    """)

    conn.commit()
    cursor.close()
    conn.close()
    os.remove(local_file)
# Définition du DAG
with DAG(
    dag_id="edu_pipeline_etl",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["bronze", "ingestion", "snowflake"],
) as dag:
    check_azure = PythonOperator(
        task_id="check_azure_connection",
        python_callable=check_azure_connection,
    )

    check_snowflake = PythonOperator(
        task_id="check_snowflake_connection",
        python_callable=check_snowflake_connection,
    )

    dbt_run = BashOperator(
    task_id='run_dbt',
    bash_command= 'cd /opt/airflow/edu_dbt && dbt run --full-refresh --profiles-dir /opt/airflow/edu_dbt/profiles',
    )
    
    for table, path in BLOB_PATHS.items():
        current_task  = PythonOperator(
            task_id=f"ingest_{table}",
            python_callable=load_csv_to_snowflake,
            op_kwargs={"table_name": table, "blob_path": path},
        )

        check_azure >> current_task
        check_snowflake >> current_task
        current_task >> dbt_run
