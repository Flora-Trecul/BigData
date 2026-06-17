import os
from pyspark.sql import SparkSession

# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Olist-Ingestion") \
    .config("spark.sql.repl.eagerEval.enabled", "true") \
    .getOrCreate()

print("Spark session started ✅")


# Load dataframes from data/0_raw directory

base_path = os.path.dirname(os.getcwd()) if os.path.basename(os.getcwd()) == "notebooks" else os.getcwd()
base_path = os.path.join(base_path, "data")

files = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "translation": "product_category_name_translation.csv",
}

dataframes = {}
i = 0
for name, file in files.items():
    df = spark.read.csv(f"{os.path.join(base_path, "0_raw", file)}", header=True, inferSchema=True, multiLine=True, escape='"')
    dataframes[name] = df
    i += 1
print(f"{i} dataframes successfully loaded ✅")


# Dataframes preview

for name, df in dataframes.items():
    print(f"\n{'='*75}")
    print(f"Table : {name.upper()}")
    print(f"Rows : {df.count()}")
    df.printSchema()



# Identify join keys

columns = {}
for name, df in dataframes.items():
    for column in df.columns:
        if column.endswith("zip_code_prefix"):
            key_name = "zip_code_prefix"
            has_prefix = True
        else:
            key_name = column
            has_prefix = False

        if key_name not in columns:
            columns[key_name] = []

        if has_prefix:
            columns[key_name].append(f"{name} ({column})")
        else:
            columns[key_name].append(name)


print("JOIN KEYS :")
for column, dfs in sorted(columns.items()):
    if len(dfs) > 1:
        print(f"- {column} : {" - ".join(dfs)}")



# Export dataframes to parquet in bronze directory

for name, df in dataframes.items():
    df.write.mode("overwrite").parquet(f"{base_path}/1_bronze/{name}")
    print(f"Dataframe {name} saved to bronze ✅")