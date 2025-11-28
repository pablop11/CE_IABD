from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, coalesce, to_timestamp, avg
from pyspark.sql.types import IntegerType, DecimalType, DoubleType

spark = SparkSession.builder.appName("silver").getOrCreate()

products = spark.read.parquet("bronze/products")

products = (products
.withColumn("id", col("id").cast(IntegerType()))
.withColumn("price" , col("price").cast(DoubleType()))
.withColumn("rating", col("rating").cast(DoubleType()))
)
products.show(100)

products.write.mode("overwrite").parquet("silver/products")

print("Silver listo")
spark.stop()
