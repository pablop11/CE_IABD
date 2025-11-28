from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum, when, date_diff, avg, to_timestamp, unix_timestamp, count, lit, concat, max, min, desc, asc
from pyspark.sql.types import IntegerType, DecimalType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gold").getOrCreate()

products = spark.read.parquet("silver/products")

marcas_frecuentes = (products
.groupBy("brand")  
.agg(count("brand").alias("frecuencia"))  
.orderBy(desc("frecuencia")).limit(5)  
)
marcas_frecuentes.show()

marcas_productos_mas_caros = (products
.groupBy("brand") 
.agg(max("price").alias("precio maximo")) 
.orderBy(desc("precio maximo"))  
)
marcas_productos_mas_caros.show()

relacion_rating_precio = (products
.withColumn("relacion", col("rating") / col("price"))
)
relacion_rating_precio.show()

categoria_valoracion_media = (products
.groupBy("category")
.agg(avg("rating").alias("valoracion media"))
.orderBy(desc("valoracion media")).limit(1)                             
)
categoria_valoracion_media.show()

productos_sobrevalorados = (products
.withColumn("sobrevalorados", col("price") / col("rating"))
.orderBy(desc("sobrevalorados"))
)
productos_sobrevalorados.show()

media_sobrevalorados = (productos_sobrevalorados
.agg(avg("sobrevalorados").alias("media sobrevalorados"))                    
)
media_sobrevalorados.show()

print("Gold listo")
spark.stop()
