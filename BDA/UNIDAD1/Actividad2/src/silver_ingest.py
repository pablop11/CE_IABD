from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, coalesce, to_timestamp
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder.appName("silver").getOrCreate()

pedidos = spark.read.parquet("bronze/pedidos")
tracking = spark.read.parquet("bronze/tracking")

pedidos = (pedidos
.withColumn("id_pedido", col("id_pedido").cast(IntegerType()))
.withColumn("id_cliente", col("id_cliente").cast(IntegerType()))
.withColumn("fecha_pedido", coalesce(
    to_date(col("fecha_pedido"), "dd/MM/yyyy"),
    to_date(col("fecha_pedido"), "yyyy/MM/dd"),
    to_date(col("fecha_pedido"), "yyyy-MM-dd")
    ))
.withColumn("fecha_prometida", coalesce(
    to_date(col("fecha_prometida"), "dd/MM/yyyy"),
    to_date(col("fecha_prometida"), "yyyy/MM/dd"),
    to_date(col("fecha_prometida"), "yyyy-MM-dd")
    ))
.dropDuplicates(["id_pedido"])
)

tracking = (tracking
.withColumn("id_pedido", col("id_pedido").cast(IntegerType()))
.withColumn("ts", coalesce(
    to_timestamp(col("ts"), "dd/MM/yyyy'T'HH:mm:ss'Z'"),
    to_timestamp(col("ts"), "yyyy/MM/dd'T'HH:mm:ss'Z'"),
    to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    ))
.dropDuplicates(["id_pedido"])
)

pedidos.show()
tracking.show()

pedidos.write.mode("overwrite").parquet("silver/pedidos")
tracking.write.mode("overwrite").parquet("silver/tracking")

print("Silver listo")
spark.stop()

