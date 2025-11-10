from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, to_timestamp, coalesce

spark = SparkSession.builder.appName("bronze").getOrCreate()

clientes = spark.read.option("header", True).csv("data/clientes.csv")
ventas = spark.read.option("header", True).csv("data/ventas_mes.csv")
facturas = spark.read.option("header", True).csv("data/facturas_meta.csv")

logs = spark.read.option("multiline", True).json("data/logs_web.json")
logs.show()

ventas.write.mode("overwrite").parquet("bronze/ventas")

ventas.show()

clientes.write.mode("overwrite").parquet("bronze/clientes")
facturas.write.mode("overwrite").parquet("bronze/facturas_meta")
logs.write.mode("overwrite").parquet("bronze/logs")

print("Bronze listo")
spark.stop()
