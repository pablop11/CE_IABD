from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum, when, date_diff, avg, to_timestamp, unix_timestamp, count, lit, concat, max, min 
from pyspark.sql.types import IntegerType, DecimalType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gold").getOrCreate()

tablets = spark.read.parquet("silver/tablets")

precio_medio = (tablets
.agg(avg("precio").alias("precio medio"))
)
precio_medio.show()

precio_minimo = (tablets
.agg(min("precio").alias("precio minimo"))                 
)
precio_minimo.show()

precio_maximo = (tablets
.agg(max("precio").alias("precio maximo"))
)
precio_maximo.show()

clasificacion = (tablets
.withColumn("categoria",when(col("precio") < 60, "barato").when((col("precio") >= 60) & (col("precio") <= 120), "medio").otherwise("caro"))
)
clasificacion.show()

top3 = (tablets
.withColumn("relacion", col("precio") / col("rating"))
)
top3 = top3.orderBy("relacion").limit(3)
top3.show()

precio_medio.write.mode("overwrite").parquet("gold/precio_medio")
precio_minimo.write.mode("overwrite").parquet("gold/precio_minimo")
precio_maximo.write.mode("overwrite").parquet("gold/precio_maximo")
clasificacion.write.mode("overwrite").parquet("gold/clasificacion")
top3.write.mode("overwrite").parquet("gold/top3")

print("Gold listo")
spark.stop()
