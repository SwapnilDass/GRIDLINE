# PySpark pipeline that processes 17M lap time records from the Ergast dataset.
# Calculates fastest lap and average lap time per driver per race, loads into PostgreSQL.

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, avg, col, round
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

DB_URL = "postgresql://gridline:gridline@host.docker.internal:5433/gridline_db"

def run_lap_analysis():
    spark = SparkSession.builder \
        .appName("GridlineLapAnalysis") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    df = spark.read.csv(
        "/opt/airflow/dataset/lap_times.csv",
        header=True,
        inferSchema=True
    )

    result = df.groupBy("raceId", "driverId") \
        .agg(
            min("milliseconds").alias("fastest_lap_ms"),
            round(avg("milliseconds"), 2).alias("avg_lap_ms")
        ) \
        .orderBy("raceId", "fastest_lap_ms")

    rows = result.collect()

    import psycopg2
    conn = psycopg2.connect("postgresql://gridline:gridline@host.docker.internal:5433/gridline_db")
    cur = conn.cursor()

    for row in rows:
        cur.execute("""
            INSERT INTO lap_analysis (race_id, driver_id, fastest_lap_ms, avg_lap_ms)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (race_id, driver_id) DO NOTHING
        """, (row.raceId, row.driverId, row.fastest_lap_ms, row.avg_lap_ms))

    conn.commit()
    cur.close()
    conn.close()
    spark.stop()


with DAG(
    dag_id="lap_time_analysis",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    analyse = PythonOperator(
        task_id="run_lap_analysis",
        python_callable=run_lap_analysis
    )