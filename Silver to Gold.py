# Databricks notebook source
# Define the path to the Parquet file in the Silver container
parquet_file_path = "/mnt/silver/transformed_data.parquet"
# Load the Parquet file into a DataFrame
df_silver = spark.read.format("parquet").load(parquet_file_path)

# COMMAND ----------

from pyspark.sql.functions import col, when

# Add a column to classify movies as "Best" or "Worst" based on IMDB Score
df_silver = df_silver.withColumn(
    "IMDB Recommendation",
    when(col("IMDB_Score") >= 7, "Best")
    .when(col("IMDB_Score") <= 5, "Worst")
    .otherwise("Average")
)

# COMMAND ----------

from pyspark.sql.functions import mean, when
# Calculate average IMDB Score
avg_imdb_score = df_silver.select(mean("IMDB_Score")).collect()[0][0]

# Fill missing IMDB scores with the average
df_silver = df_silver.withColumn("IMDB_Score", when(df_silver["IMDB_Score"].isNull(), avg_imdb_score).otherwise(df_silver["IMDB_Score"]))

# COMMAND ----------

from pyspark.sql.functions import split, explode

# Split genres by "/"
df_silver = df_silver.withColumn("Genres", split("Genre", "/"))
df_silver = df_silver.withColumn("Genre", explode("Genres")).drop("Genres")

# COMMAND ----------

from pyspark.sql.functions import col, split

# Split the 'Language' column by '/' and retain only the first part
df_silver = df_silver.withColumn(
    "Language",
    split(col("Language"), "/").getItem(0)
)

# COMMAND ----------

import re

# Function to clean column names
def clean_column_name(name):
    # Remove leading/trailing whitespace
    name = name.strip()
    # Replace spaces with underscores
    name = name.replace(" ", "_")
    # Remove invalid characters (e.g., invisible ones)
    name = re.sub(r'[^\w]', '', name)  # Keep only alphanumeric and underscores
    return name.lower()  # Optional: Convert to lowercase for consistency

# Rename all columns
cleaned_columns = {col: clean_column_name(col) for col in df_silver.columns}
for old_name, new_name in cleaned_columns.items():
    df_silver = df_silver.withColumnRenamed(old_name, new_name)
# Verify the cleaned column names

# COMMAND ----------

dbutils.fs.rm(output_path_delta, recurse=True)
# Define the output path in the Gold container
output_path_delta = "/mnt/gold/transformed_data.delta"
df_silver.write.format("delta").save(output_path_delta)
