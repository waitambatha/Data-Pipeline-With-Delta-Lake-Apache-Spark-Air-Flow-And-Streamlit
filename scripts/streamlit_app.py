import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StreamlitApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .getOrCreate()

# Load data from Delta Lake
delta_path = "data/delta_tables/ev_population"
df = spark.read.format("delta").load(delta_path)

# Convert Spark DataFrame to Pandas DataFrame for visualization
pandas_df = df.toPandas()

# Streamlit App
st.title("Electric Vehicle Population Data Visualization")
st.write("Visualizing Electric Vehicle Population Data in Washington State")

# Display raw data
st.subheader("Raw Data")
st.write(pandas_df.head(100))  # Show first 100 rows

# Visualization 1: Bar Chart (Count of Vehicles by Make)
st.subheader("1. Count of Vehicles by Make")
fig1 = px.bar(pandas_df["Make"].value_counts(), title="Count of Vehicles by Make")
st.plotly_chart(fig1)

# Visualization 2: Pie Chart (Electric Vehicle Type Distribution)
st.subheader("2. Electric Vehicle Type Distribution")
fig2 = px.pie(pandas_df, names="Electric_Vehicle_Type", title="Electric Vehicle Type Distribution")
st.plotly_chart(fig2)

# Visualization 3: Histogram (Electric Range Distribution)
st.subheader("3. Electric Range Distribution")
fig3 = px.histogram(pandas_df, x="Electric_Range", title="Electric Range Distribution")
st.plotly_chart(fig3)

# Visualization 4: Scatter Plot (Electric Range vs Model Year)
st.subheader("4. Electric Range vs Model Year")
fig4 = px.scatter(pandas_df, x="Model_Year", y="Electric_Range", title="Electric Range vs Model Year")
st.plotly_chart(fig4)

# Visualization 5: Box Plot (Electric Range by Make)
st.subheader("5. Electric Range by Make")
fig5 = px.box(pandas_df, x="Make", y="Electric_Range", title="Electric Range by Make")
st.plotly_chart(fig5)

# Visualization 6: Heatmap (Correlation Matrix)
st.subheader("6. Correlation Matrix")
corr = pandas_df.corr(numeric_only=True)
fig6, ax = plt.subplots()
sns.heatmap(corr, annot=True, ax=ax)
st.pyplot(fig6)

# Visualization 7: Area Chart (Count of Vehicles by Model Year)
st.subheader("7. Count of Vehicles by Model Year")
fig7 = px.area(pandas_df["Model_Year"].value_counts(), title="Count of Vehicles by Model Year")
st.plotly_chart(fig7)

# Visualization 8: Violin Plot (Electric Range by Electric Vehicle Type)
st.subheader("8. Electric Range by Electric Vehicle Type")
fig8 = px.violin(pandas_df, x="Electric_Vehicle_Type", y="Electric_Range", title="Electric Range by Electric Vehicle Type")
st.plotly_chart(fig8)

# Visualization 9: Pair Plot (Relationships Between Numeric Columns)
st.subheader("9. Pair Plot")
fig9 = sns.pairplot(pandas_df[["Model_Year", "Electric_Range", "Base_MSRP"]])
st.pyplot(fig9)

# Visualization 10: Count Plot (CAFV Eligibility Distribution)
st.subheader("10. CAFV Eligibility Distribution")
fig10, ax = plt.subplots()
sns.countplot(x="CAFV_Eligibility", data=pandas_df, ax=ax)
st.pyplot(fig10)

# Visualization 11: Density Plot (Electric Range Distribution)
st.subheader("11. Electric Range Distribution")
fig11, ax = plt.subplots()
sns.kdeplot(pandas_df["Electric_Range"], ax=ax)
st.pyplot(fig11)

# Visualization 12: 3D Scatter Plot (Model Year, Electric Range, and Base MSRP)
st.subheader("12. 3D Scatter Plot")
fig12 = px.scatter_3d(pandas_df, x="Model_Year", y="Electric_Range", z="Base_MSRP", title="3D Scatter Plot")
st.plotly_chart(fig12)

# Visualization 13: Sunburst Chart (Make, Model, and Electric Vehicle Type)
st.subheader("13. Sunburst Chart")
fig13 = px.sunburst(pandas_df, path=["Make", "Model", "Electric_Vehicle_Type"], values="Electric_Range", title="Sunburst Chart")
st.plotly_chart(fig13)

# Visualization 14: Treemap (Make and Model Distribution)
st.subheader("14. Treemap")
fig14 = px.treemap(pandas_df, path=["Make", "Model"], values="Electric_Range", title="Treemap")
st.plotly_chart(fig14)

# Visualization 15: Line Chart (Electric Range Over Time)
st.subheader("15. Electric Range Over Time")
fig15 = px.line(pandas_df, x="Model_Year", y="Electric_Range", title="Electric Range Over Time")
st.plotly_chart(fig15)