from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

#Start a Spark Session with 1GB memory for the driver standings  job
def run_driver_standings():
    spark = SparkSession.builder.appName("DriverStandings").config("spark.driver.memory", "1g").getOrCreate()

    #Read driver standings CSV into a Spark DataFrame
    standings = spark.read.csv("/opt/airflow/dataset/driver_standings.csv",
        header = True,
        inferSchema = True
    )

    #Read drivers CSV into a Spark DataFrame
    drivers = spark.read.csv("/opt/airflow/dataset/drivers.csv",
        header=True,
        inferSchema=True
    )


    result = standings.groupBy("driverId").agg(
        sum("points").alias("total_points"),
        sum("wins").alias("total_wins")
    ).join(
        drivers.select("driverId", "forename", "surname"
        ),
        on = "driverId"
    ).orderBy(col("total_points").desc()).limit(15)

    rows = result.collect()

    import psycopg2
    conn = psycopg2.connect("postgresql://gridline:gridline@host.docker.internal:5433/gridline_db")
    cur = conn.cursor()

    for row in rows:
        cur.execute("""
            INSERT INTO driver_standings (driver_id, forename, surname, total_points, total_wins)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (driver_id) DO NOTHING
        """, (row.driverId, row.forename, row.surname, row.total_points, row.total_wins))

    conn.commit()
    cur.close()
    conn.close()
    spark.stop()

with DAG(
    dag_id="driver_standings",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="run_driver_standings",
        python_callable=run_driver_standings
    )
    
