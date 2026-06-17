import os 
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Olist-Ingestion") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

Files = {
    "orders":      "data/0_raw/olist_orders_dataset.csv",
    "customers":   "data/0_raw/olist_customers_dataset.csv",
    "order_items": "data/0_raw/olist_order_items_dataset.csv",
    "payments":    "data/0_raw/olist_order_payments_dataset.csv",
    "products":    "data/0_raw/olist_products_dataset.csv",
    "sellers":     "data/0_raw/olist_sellers_dataset.csv",
    "reviews":     "data/0_raw/olist_order_reviews_dataset.csv",
    "geolocation": "data/0_raw/olist_geolocation_dataset.csv",
    "translation": "data/0_raw/product_category_name_translation.csv",
}

dataframes = {}
for name, path in Files.items() : 
    df = spark.read.csv(path, header=True, inferSchema=True)
    dataframes[name] = df
    print(f"\n{'='*50}")
    print(f"Table : {name}")
    print(f"Lignes : {df.count()}")
    print(f"Colonnes : {df.columns}")
    df.printSchema()


table_names = list(dataframes.keys())

for i in range(len(table_names)):
    for j in range(i + 1, len(table_names)):
        name_a = table_names[i]
        name_b = table_names[j]
        cols_a = set(dataframes[name_a].columns)
        cols_b = set(dataframes[name_b].columns)
        common = cols_a & cols_b
        if common:
            print(f"{name_a} ↔ {name_b} : {common}")    


for name, path in Files.items():
    df = spark.read.csv(path, header=True, inferSchema=True)
    df.write.mode("overwrite").parquet(f"data/1_bronze/{name}")

print("\nBronze saved ")    