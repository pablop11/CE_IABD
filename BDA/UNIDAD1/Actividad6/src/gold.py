from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum, when, date_diff, avg, to_timestamp, unix_timestamp, count, lit, concat, max, min, desc, asc
from pyspark.sql.types import IntegerType, DecimalType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gold").getOrCreate()

library = spark.read.parquet("silver/library")
authors = spark.read.parquet("silver/authors")
language = spark.read.parquet("silver/language")
books = spark.read.parquet("silver/books")


publicacion_por_años = (books
.groupBy("first_publish_year")  
.agg(count("first_publish_year").alias("cantidad"))  
.orderBy(desc("cantidad"))  
)
publicacion_por_años.show(100)

autores_mas_frecuentes = (authors
.groupBy("author")  
.agg(count("author").alias("frecuencia"))  
.orderBy(desc("frecuencia"))  
)
autores_mas_frecuentes.show(100)

libros_mas_reeditados = (books
.orderBy(desc("edition_count"))  
)
libros_mas_reeditados.show(100)

idiomas_que_predominan = (language
.groupBy("language")  
.agg(count("language").alias("frecuencia"))  
.orderBy(desc("frecuencia")) 
)
idiomas_que_predominan.show(100)

print("Gold listo")
spark.stop()
