from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum, when, date_diff, avg, to_timestamp, unix_timestamp, count, lit, concat, max, min
from pyspark.sql.types import IntegerType, DecimalType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gold").getOrCreate()

libros = spark.read.parquet("silver/books")

precio_medio = (libros
.agg(avg("precio").alias("precio medio"))
)
precio_medio.show()

precio_minimo = (libros
.agg(min("precio").alias("precio minimo"))                 
)
precio_minimo.show()

precio_maximo = (libros
.agg(max("precio").alias("precio minimo"))
)
precio_maximo.show()

top5_libros_mas_caros = libros.orderBy("precio", ascending=False).limit(5)
top5_libros_mas_caros.show()

precio_medio.write.mode("overwrite").parquet("gold/precio_medio")
precio_minimo.write.mode("overwrite").parquet("gold/precio_minimo")
precio_maximo.write.mode("overwrite").parquet("gold/precio_maximo")
top5_libros_mas_caros.write.mode("overwrite").parquet("gold/top5_libros_mas_caros")

print("Gold listo")
spark.stop()
