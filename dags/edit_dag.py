from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from typing import Dict
from datetime import datetime
import pandas as pd
##################################################################
#Hooks
##################################################################

postgres_hook = PostgresHook(postgres_conn_id="postgres")

##################################################################
#Variables
##################################################################

##################################################################
#Tasks
##################################################################

def _success_criteria(record):
    return record


def _failure_criteria(record):
    return True if not record else False

def _update_zip_codes():
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_name = 'check_zips'
        );
    """)
    table_exists = cursor.fetchone()[0]
    if table_exists > 0:
        cursor.execute("TRUNCATE TABLE clima.check_zips;")
        print('Table truncated')
        cursor.execute("""
        INSERT INTO clima.check_zips (
            SELECT * 
            FROM clima.zip_coordinates
        );
    """)

    else:
        print('Table does not exist')
        #create table if not exists
        cursor.execute("""CREATE TABLE IF NOT EXISTS clima.check_zips AS
            SELECT * 
            FROM clima.zip_coordinates
        );""")
    conn.commit()
    cursor.close()
    conn.close()

def validation_function():
    return {"New rows found"}
    
with DAG(
    dag_id="postgres_sensor",
    description="DAG in charge of checking new zip codes",
    start_date=datetime(2025, 1, 1),
    schedule='@hourly',
    catchup=False,
    tags=["postgres", "sensor", "climate"],
) as dag:
    start_task = EmptyOperator(task_id="start_task")

    end_task = EmptyOperator(task_id="end_task")
#sensor that checks if there are new rows in the zip_coordinates table, we use an additional table named check_zips to store the previous state of the table
#This help us to determine if there are new rows in the table
#we use the success and failure criteria to determine if the task should continue or fail
    waiting_for_partner = SqlSensor(
        task_id="waiting_for_rows",
        conn_id="postgres",
        sql="sql/check_new_zips.sql",
        parameters={},
        success=_success_criteria,
        failure=_failure_criteria,
        fail_on_empty=False,
        poke_interval=20,
        #mode="reschedule",
        timeout=60,
    )
#in case there is a new row we update our check_zips table
    update_zip = PythonOperator(
        task_id="update_zip_codes", python_callable=_update_zip_codes
    )

    validation = PythonOperator(
        task_id="validation", python_callable=validation_function
    )

    # Define a downstream task to trigger another DAG
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_another_dag',
        trigger_dag_id='historic_data_dag', 
    )

    start_task >> waiting_for_partner >> validation >> update_zip >> trigger_dag >> end_task