from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_constructor_standings():
    spark = SparkSession.builder \
        .appName("ConstructorStandings") \
        .config("spark.driver.memory", "1g")\
        .getOrCreate()
    
    standings = spark.read.csv(
        "/opt/airflow/dataset/constructor_standings.csv",
        header=True,
        inferSchema=True
    )

    constructors = spark.read.csv(
        "/opt/airflow/dataset/constructors.csv",
        header=True,
        inferSchema=True
    )

    result = standings.groupBy("constructorId") \
        .agg(
            sum("points").alias("total_points"),
            sum("wins").alias("total_wins")
        ) \
        .join(constructors.select("constructorId", "name"), on="constructorId") \
        .orderBy(col("total_points").desc()) \
        .limit(20)

    rows = result.collect()

    import psycopg2
    conn = psycopg2.connect("postgresql://gridline:gridline@host.docker.internal:5433/gridline_db")
    cur = conn.cursor()

    for row in rows:
        cur.execute("""
            INSERT INTO constructor_standings (constructor_id, name, total_points, total_wins)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (constructor_id) DO NOTHING
        """, (row.constructorId, row.name, row.total_points, row.total_wins))

        conn.commit()
    cur.close()
    conn.close()
    spark.stop()

with DAG(
    dag_id="constructor_standings",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="run_constructor_standings",
        python_callable=run_constructor_standings
    )

