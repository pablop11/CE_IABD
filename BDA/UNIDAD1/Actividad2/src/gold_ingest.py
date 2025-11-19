from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum, when, date_diff, avg, to_timestamp, unix_timestamp, count, lit, concat
from pyspark.sql.types import IntegerType, DecimalType
from pyspark.sql import functions as F

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

join_pedidos_tiempocompleto = pedidos.join(tracking_sinnull, "id_pedido")
tiempo_tardado = (join_pedidos_tiempocompleto
.withColumn("fecha_pedido", to_timestamp("fecha_pedido"))
.withColumn("diferencia_segundos", (unix_timestamp("ts") - unix_timestamp("fecha_pedido")))
.withColumn("dias", (col("diferencia_segundos") / 86400).cast("int"))  # 86400 segundos en un día
.withColumn("horas", ((col("diferencia_segundos") % 86400) / 3600).cast("int"))  # 3600 segundos en una hora
.withColumn("minutos", ((col("diferencia_segundos") % 3600) / 60).cast("int"))  # 60 segundos en un minuto
.withColumn("tiempototal", concat(
    col("dias").cast("string"), lit(" días, "), 
    col("horas").cast("string"), lit(" horas, "), 
    col("minutos").cast("string"), lit(" minutos")))
.select("id_pedido", "fecha_pedido", "ts", "tiempototal")
)

print("TIEMPO QUE HA TARDADO EL PEDIDO EN LLEGAR:")
tiempo_tardado.show()

# Filtramos las entregas a tiempo (dias_retraso <= 0)
entregas_a_tiempo = restar_fechas.filter(col("dias_retraso") <= 0)

# Realizamos la agregación para contar las entregas totales y las entregas a tiempo
porcentaje_entregas_a_tiempo = (restar_fechas
.agg(count("*").alias("total_entregas"),  # Contamos el total de entregas
     count(when(col("dias_retraso") <= 0, 1)).alias("entregas_a_tiempo")  # Contamos las entregas a tiempo
)
# Calculamos el porcentaje de entregas a tiempo
.withColumn("porcentaje_entregas_a_tiempo", (col("entregas_a_tiempo") / col("total_entregas")) * 100)
)

print("PORCENTAJE DE ENTREGAS A TIEMPO:")
porcentaje_entregas_a_tiempo.show()
    
print("Gold listo")
spark.stop()
