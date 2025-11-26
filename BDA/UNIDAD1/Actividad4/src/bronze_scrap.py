import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("bronze-scraping").getOrCreate()
base_url = "https://webscraper.io/test-sites/e-commerce/static/computers/tablets?page="

data = []
page_num = 0

# Iterar sobre las páginas 
while True:
    # Crear la URL completa para la página actual
    page_num += 1
    URL = base_url + str(page_num)

    # Realizar la solicitud GET para obtener el HTML
    response = requests.get(URL)
    response.encoding = "utf-8"
    html = response.text

    # Parsear el HTML con BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Seleccionar todos los divs con la clase 'product-wrapper' (que es el contenedor de cada tablet)
    tablets = soup.select("div.product-wrapper")

    # Si no hay tablets termina el bucle while y se ejecuta el for
    if not tablets:
        print(f"No se encontraron tablets en la página {page_num}. Fin del scraping.")
        break

    # Verificar si se están seleccionando correctamente los productos
    print(f"Se encontraron {len(tablets)} tablets en la página {page_num}.")

    # Iterar sobre cada producto y extraer el nombre, el precio y el rating
    for t in tablets:
        try:
            # Obtener el h4 que contiene el <a> (el nombre del producto)
            title_tag = t.select_one("h4 a.title")
            if title_tag and "title" in title_tag.attrs:
                title = title_tag["title"]
            else:
                title = "Nombre no disponible"

            # Obtener el precio desde el <span> con itemprop="price"
            price_tag = t.select_one("span[itemprop='price']")
            if price_tag:
                price_raw = price_tag.get_text().strip()
                price = float(price_raw.replace('$', '').replace(',', ''))
            else:
                price = None

            # Obtener el rating (data-rating) si está presente
            rating_tag = t.select_one("p[data-rating]")
            if rating_tag and "data-rating" in rating_tag.attrs:
                rating = rating_tag["data-rating"]
            else:
                rating = None

            data.append((title, price, rating))

        except Exception as e:
            print("Error leyendo tablet:", e)

# =======================================================
# 2) SCHEMA + DATAFRAME
# =======================================================

# Definir el esquema para el DataFrame
schema = StructType([
    StructField("titulo", StringType(), True),
    StructField("precio", DoubleType(), True),
    StructField("rating", StringType(), True)
])

df_scraping = spark.createDataFrame(data, schema)

# =======================================================
# 3) GUARDAR BRONZE
# =======================================================

df_scraping.write.mode("overwrite").parquet("bronze/tablets")
df_scraping.show()

print("BRONZE scraping listo.")

spark.stop()

