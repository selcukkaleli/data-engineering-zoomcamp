import pyspark
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

print(f"Spark version: {spark.version}")

# Spark dataframe oluştur
df = spark.range(10)

# sonucu göster
df.show()
time.sleep(60)

spark.stop()