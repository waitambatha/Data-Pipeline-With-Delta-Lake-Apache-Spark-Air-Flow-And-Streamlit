from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import subprocess

# Initialize Spark session
def init_spark():
    return SparkSession.builder \
        .appName("FetchAndLoadEVData") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .getOrCreate()

# Fetch data from the API
def fetch_data():
    url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

# Load data into Delta Lake
def load_to_delta():
    spark = init_spark()
    data = fetch_data()

    # Extract the rows from the JSON response
    rows = data["data"]

    # Extract only the relevant columns (skip metadata)
    cleaned_rows = [row[8:25] for row in rows]

    # Define schema for the dataset
    schema = StructType([
        StructField("VIN", StringType(), True),
        StructField("County", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal_Code", StringType(), True),
        StructField("Model_Year", IntegerType(), True),
        StructField("Make", StringType(), True),
        StructField("Model", StringType(), True),
        StructField("Electric_Vehicle_Type", StringType(), True),
        StructField("CAFV_Eligibility", StringType(), True),
        StructField("Electric_Range", IntegerType(), True),
        StructField("Base_MSRP", IntegerType(), True),
        StructField("Legislative_District", IntegerType(), True),
        StructField("DOL_Vehicle_ID", IntegerType(), True),
        StructField("Vehicle_Location", StringType(), True),
        StructField("Electric_Utility", StringType(), True),
        StructField("2020_Census_Tract", StringType(), True)
    ])

    # Convert cleaned data to a list of tuples with correct data types
    typed_rows = []
    for row in cleaned_rows:
        try:
            typed_row = (
                row[0],  # VIN
                row[1],  # County
                row[2],  # City
                row[3],  # State
                row[4],  # Postal Code
                int(row[5]) if row[5] else None,  # Model Year
                row[6],  # Make
                row[7],  # Model
                row[8],  # Electric Vehicle Type
                row[9],  # CAFV Eligibility
                int(row[10]) if row[10] else None,  # Electric Range
                int(row[11]) if row[11] else None,  # Base MSRP
                int(row[12]) if row[12] else None,  # Legislative District
                int(row[13]) if row[13] else None,  # DOL Vehicle ID
                row[14],  # Vehicle Location
                row[15],  # Electric Utility
                row[16]   # 2020 Census Tract
            )
            typed_rows.append(typed_row)
        except (ValueError, TypeError) as e:
            print(f"Skipping row due to error: {e}. Row: {row}")

    # Convert cleaned data to Spark DataFrame
    df = spark.createDataFrame(typed_rows, schema=schema)

    # Write data to Delta Lake
    delta_path = "data/delta_tables/ev_population"
    df.write.format("delta").mode("overwrite").save(delta_path)
    print(f"Data loaded into Delta Lake at '{delta_path}'!")

# Trigger Streamlit app
def trigger_streamlit():
    subprocess.run(["streamlit", "run", "streamlit_app.py"])

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'ev_pipeline',
    default_args=default_args,
    description='Fetch EV data, load into Delta Lake, and trigger Streamlit app',
    schedule_interval='@daily',
)

# Define tasks
fetch_and_load_task = PythonOperator(
    task_id='fetch_and_load',
    python_callable=load_to_delta,
    dag=dag,
)

trigger_streamlit_task = PythonOperator(
    task_id='trigger_streamlit',
    python_callable=trigger_streamlit,
    dag=dag,
)

# Set task dependencies
fetch_and_load_task >> trigger_streamlit_task