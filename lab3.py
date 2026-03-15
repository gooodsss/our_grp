from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Lab3").getOrCreate()

df = spark.read.vsc("housing.csv", header=True, inferSchema=True)

print("Original Dataset:")
df.show(5)

df_clean = df.dropna()

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

#SQL temporary view
df_clean.createOrReplaceTempView("housing_data")

#top 5 most frequent ocean proximity categories
result = spark.sql("""
    SELECT ocean_proximity, COUNT(*) AS total_houses
    FROM housing_data
    GROUP BY ocean_proximity
    ORDER BY total_houses DESC
    LIMIT 5
""")

print("Top 5 Ocean Proximity Categories:")
result.show()

result.write.csv("overwiew").csv("output/top_proximity.csv", header=True)
result.write.json("overview").json("output/top_proximity.json")
result.write.mode("overwrite").format("text").save("output/top_proximity.txt")

spark.stop()