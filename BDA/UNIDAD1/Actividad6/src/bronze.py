import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

spark = SparkSession.builder.appName("bronze-scraping").getOrCreate()

URL = "https://openlibrary.org/search.json?q=python"

response = requests.get(URL).json()
library = response["docs"]

data = []

for l in library:
    data.append((
    l.get("title"), 
    l.get("author_name"), 
    l.get("first_publish_year"), 
    l.get("edition_count"), 
    l.get("language"), 
    l.get("cover_i")))

schema = StructType([
    StructField("title", StringType(), True),
    StructField("author_name", ArrayType(StringType()), True),
    StructField("first_publish_year", IntegerType(), True),
    StructField("edition_count", IntegerType(), True),
    StructField("language", ArrayType(StringType()), True),
    StructField("cover_i", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

df.write.mode("overwrite").parquet("bronze/library")
df.show()

print("BRONZE listo.")

spark.stop()
