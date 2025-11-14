from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, to_timestamp, coalesce

spark = SparkSession.builder.appName("bronze").getOrCreate()

pedidos = spark.read.option("header", True).option("delimiter", ";").csv("data/pedidos.csv")
pedidos.show()

tracking = spark.read.option("multiline", True).json("data/tracking.json")
tracking.show()

pedidos.write.mode("overwrite").parquet("bronze/pedidos")
tracking.write.mode("overwrite").parquet("bronze/tracking")

print("Bronze listo")
spark.stop()