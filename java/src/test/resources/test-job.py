from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print("Hello from kyuubi-submit Java client test!")
spark.stop()
