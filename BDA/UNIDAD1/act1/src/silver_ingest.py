from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder.appName("silver").getOrCreate()

ventas = spark.read.parquet("bronze/ventas")
clientes = spark.read.parquet("bronze/clientes")
facturas = spark.read.parquet("bronze/facturas_meta")

ventas = (ventas
.withColumn("id_venta", col("id_venta").cast(IntegerType()))
.withColumn("fecha", to_date(col("fecha")))
.withColumn("id_cliente", col("id_cliente").cast(IntegerType()))
.withColumn("unidades", col("unidades").cast(IntegerType()))
.withColumn("importe", col("importe").cast(DecimalType(10, 2)))
.dropDuplicates(["id_venta"])
)

ids = clientes.select(col("id_cliente").cast(IntegerType()).alias("id_cliente"))

ventas_ok = (ventas
.join(ids, "id_cliente", "inner")
.where((col("unidades") > 0) & (col("importe") >= 0))
)

clientes = (clientes
.withColumn("id_cliente", col("id_cliente").cast(IntegerType()))
.withColumn("fecha_alta", to_date(col("fecha_alta")))
.dropDuplicates(["id_cliente"])
)

facturas = (facturas
.withColumn("id_cliente", col("id_cliente").cast(IntegerType()))
.withColumn("importe_total", col("importe_total").cast(DecimalType()))
.filter(col("importe_total") > 0)
.withColumn("fecha", to_date(col("fecha")))
.dropDuplicates(["id_factura"])
)

ventas_ok.write.mode("overwrite").parquet("silver/ventas")
clientes.write.mode("overwrite").parquet("silver/clientes")
facturas.write.mode("overwrite").parquet("silver/facturas_meta")

print("Silver listo")
spark.stop()
