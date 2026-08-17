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

    
