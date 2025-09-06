# Databricks notebook source
dbutils.fs.ls("/mnt/bronze/")
csv_file_path = f"/mnt/bronze/dbo.data.txt"
# Load the CSV file into a DataFrame
df_csv = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load(csv_file_path)
# Display the schema and some data to verify

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, to_date, year, avg, round

# COMMAND ----------

df_csv = df_csv.withColumn("IMDB_Score", round(col("IMDB_Score").cast("double"), 1))


# COMMAND ----------

df_csv = df_csv.withColumn("ReleaseDate", to_date(col("ReleaseDate"), "yyyy-MM-dd"))
df_csv = df_csv.withColumn("AddedDate", to_date(col("AddedDate"), "yyyy-MM-dd"))
df_csv = df_csv.withColumn("ReleaseYear", year(col("ReleaseDate")))


# COMMAND ----------

# Step 1: Handle Missing Values
# Replace NULL values in the 'Language' column with "English"
df_csv = df_csv.withColumn("Language", when(col("Language").isNull(), "English").otherwise(col("Language")))

# Replace NULL values in the 'Runtime' column with 0
df_csv = df_csv.withColumn("Runtime", when(col("Runtime").isNull(), 0).otherwise(col("Runtime")))

# Replace NULL values in the 'Views' column with 0
df_csv = df_csv.withColumn("Views", when(col("Views").isNull(), 0).otherwise(col("Views")))

# Replace NULL values in the 'Title' column with "NexusDatalens null value"
df_csv = df_csv.withColumn("Title", when(col("Title").isNull(), "No Title").otherwise(col("Title")))

# COMMAND ----------

output_path_parquet = "/mnt/silver/transformed_data.parquet"

# Save the transformed DataFrame as Parquet
df_csv.write.format("parquet").mode("overwrite").save(output_path_parquet)