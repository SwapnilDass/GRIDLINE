# ETL pipeline that reads drivers.csv from the Ergast dataset and loads it into the PostgreSQL drivers table.
# Run manually from the Airflow UI — not scheduled.


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2


DB_URL = "postgresql://gridline:gridline@host.docker.internal:5433/gridline_db"



def load_drivers():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    df = pd.read_csv("/opt/airflow/dataset/drivers.csv", na_values=["\\N"])

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO drivers (driver_id, driver_ref, permanent_number, code, forename, surname, dob, nationality)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (driver_id) DO NOTHING
        """, (
            row.driverId,
            row.driverRef,
            None if pd.isna(row.get("number")) else int(float(row.get("number"))),
            None if pd.isna(row.get("code")) else row.get("code"),
            row.forename,
            row.surname,
            None if pd.isna(row.get("dob")) else row.get("dob"),
            row.nationality
        ))
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id="load_ergast_data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    load_drivers_task = PythonOperator(
        task_id="load_drivers",
        python_callable=load_drivers
    )
