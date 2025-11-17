from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum, when, date_diff, avg, to_timestamp, unix_timestamp
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder.appName("gold").getOrCreate()

pedidos = spark.read.parquet("silver/pedidos")
tracking = spark.read.parquet("silver/tracking")

tracking_ordenado = (tracking
.withColumn("fecha_entrega", when(col("evento") == "delivered", to_date(col("ts"))).otherwise(None))
)
tracking_ordenado.show()

tracking_sinnull = tracking_ordenado.filter(col("fecha_entrega").isNotNull())
tracking_sinnull.show()

join_pedidos = pedidos.join(tracking_sinnull, "id_pedido", "right")
join_pedidos.show()

restar_fechas = (join_pedidos
.withColumn("dias_retraso", date_diff("fecha_entrega", "fecha_prometida"))
.select("id_pedido", "fecha_prometida", "fecha_entrega", "dias_retraso")
)
restar_fechas.show()

media_dias = (restar_fechas
.agg(avg("dias_retraso").alias("media_dias"))
)
print("MEDIA DE DIAS DE RETRASO:")
media_dias.show()

join_pedidos_tiempocompleto = pedidos.join(tracking, "id_pedido")
tiempo_tardado = (join_pedidos_tiempocompleto
.withColumn("ts", to_timestamp("ts").alias("fecha_entrega"))
)
tiempo_tardado.show()


print("Gold listo")
spark.stop()
