from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, coalesce, to_timestamp, avg
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder.appName("silver").getOrCreate()

libros = spark.read.parquet("bronze/books")

libros = (libros
.withColumn("precio", col("precio").cast(DecimalType(10, 2)))
)

libros.show()

libros.write.mode("overwrite").parquet("silver/books")

print("Silver listo")
spark.stop()

