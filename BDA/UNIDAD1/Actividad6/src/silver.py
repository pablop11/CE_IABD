from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, coalesce, to_timestamp, avg, explode, upper, trim
from pyspark.sql.types import IntegerType, DecimalType, DoubleType

spark = SparkSession.builder.appName("silver").getOrCreate()

library = spark.read.parquet("bronze/library")

authors = (library
.select(col("cover_i"), explode(col("author_name")).alias("author"))  
.withColumn("author", upper(trim(col("author"))))  
)
authors.show(100)

language = (library
.select(col("cover_i"), explode(col("language")).alias("language"))  
.withColumn("language", upper(col("language")))  
)
language.show(100)

books = (library
.select("title", "first_publish_year", "edition_count"))
books.show(100)

library.write.mode("overwrite").parquet("silver/library")
authors.write.mode("overwrite").parquet("silver/authors")
language.write.mode("overwrite").parquet("silver/language")
books.write.mode("overwrite").parquet("silver/books")

print("Silver listo")
spark.stop()
