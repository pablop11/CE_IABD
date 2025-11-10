from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum 
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder.appName("gold").getOrCreate()

ventas = spark.read.parquet("silver/ventas")

facturas = spark.read.parquet("silver/facturas_meta")

ventas_por_dia = (ventas.groupBy("fecha").agg(
sum("importe").alias("ventas_totales"),
sum("unidades").alias("unidades_totales")
).orderBy("fecha"))

ventas_por_dia.write.mode("overwrite").parquet("gold/ventas_por_dia")

top5 = ventas.groupBy("id_producto").agg(
sum("unidades").alias("unidades_totales"),
sum("importe").alias("importe_total")
).orderBy(col("unidades_totales").desc()).limit(5)

top5.write.mode("overwrite").parquet("gold/top5_productos")

print("Gold listo")
spark.stop()
