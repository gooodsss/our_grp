from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Lab3").getOrCreate()

df = spark.read.vsc("housing.csv", header=True, inferSchema=True)

print("Original Dataset:")
df.show(5)

selected_df = df_clean.select(
    "median_income",
    "median_house_value",
    "ocean_proximity",
    "population"
)

#Filter houses with income greater than 5
filtered_df = selected_df.filter(selected_df.median_income > 5)

print("Filtered Dataset:")
filtered_df.show(5)

