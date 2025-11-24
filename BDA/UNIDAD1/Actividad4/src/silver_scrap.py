from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, coalesce, to_timestamp, avg
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder.appName("silver").getOrCreate()

tablets = spark.read.parquet("bronze/tablets")

tablets = (tablets
.withColumn("precio", col("precio").cast(DecimalType(10, 2)))
)

tablets.show()

tablets.write.mode("overwrite").parquet("silver/tablets")

print("Silver listo")
spark.stop()
