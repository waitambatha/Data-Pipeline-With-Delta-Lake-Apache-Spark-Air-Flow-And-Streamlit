import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
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
        print("JSON Response Keys:", data.keys())  # Debug: Print keys in the JSON response
        print("First Row of Data:", data["data"][0])  # Debug: Print the first row of data
        return data
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

# Load data into Delta Lake
def load_to_delta(data):
    # Extract the rows from the JSON response
    rows = data["data"]

    # Extract only the relevant columns (skip metadata)
    cleaned_rows = [row[8:25] for row in rows]  # Adjust the slice to match the actual data

    # Define schema for the dataset
    schema = StructType([
        StructField("VIN", StringType(), True),
        StructField("County", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal_Code", StringType(), True),  # Replace space with underscore
        StructField("Model_Year", IntegerType(), True),  # Replace space with underscore
        StructField("Make", StringType(), True),
        StructField("Model", StringType(), True),
        StructField("Electric_Vehicle_Type", StringType(), True),  # Replace spaces with underscores
        StructField("CAFV_Eligibility", StringType(), True),  # Replace spaces and parentheses with underscores
        StructField("Electric_Range", IntegerType(), True),  # Replace space with underscore
        StructField("Base_MSRP", IntegerType(), True),  # Replace space with underscore
        StructField("Legislative_District", IntegerType(), True),  # Replace space with underscore
        StructField("DOL_Vehicle_ID", IntegerType(), True),  # Replace spaces with underscores
        StructField("Vehicle_Location", StringType(), True),  # Replace space with underscore
        StructField("Electric_Utility", StringType(), True),  # Replace space with underscore
        StructField("2020_Census_Tract", StringType(), True)  # Replace space with underscore
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
                int(row[5]) if row[5] else None,  # Model Year (convert to integer, handle None)
                row[6],  # Make
                row[7],  # Model
                row[8],  # Electric Vehicle Type
                row[9],  # CAFV Eligibility
                int(row[10]) if row[10] else None,  # Electric Range (convert to integer, handle None)
                int(row[11]) if row[11] else None,  # Base MSRP (convert to integer, handle None)
                int(row[12]) if row[12] else None,  # Legislative District (convert to integer, handle None)
                int(row[13]) if row[13] else None,  # DOL Vehicle ID (convert to integer, handle None)
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

# Main function
if __name__ == "__main__":
    data = fetch_data()
    load_to_delta(data)