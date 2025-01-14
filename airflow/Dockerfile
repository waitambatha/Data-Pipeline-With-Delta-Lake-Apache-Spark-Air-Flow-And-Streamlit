FROM apache/airflow:2.5.1

# Install pyspark and other dependencies as the airflow user
USER airflow
RUN pip install --user pyspark==3.4.0 delta-spark==2.4.0