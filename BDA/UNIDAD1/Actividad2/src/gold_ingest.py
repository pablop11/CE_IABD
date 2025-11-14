from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum 
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder.appName("gold").getOrCreate()

pedidos = spark.read.parquet("silver/pedidos")
tracking = spark.read.parquet("silver/tracking")




print("Gold listo")
spark.stop()
