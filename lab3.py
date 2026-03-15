from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Lab3").getOrCreate()

df = spark.read.vsc("housing.csv", header=True, inferSchema=True)

print("Original Dataset:")
df.show(5)

