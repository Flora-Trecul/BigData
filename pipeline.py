import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, ShortType
from pyspark.sql import functions as F


os.environ["SPARK_CONNECT_MODE_ENABLED"] = "0"

if sys.platform == "darwin":
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
elif sys.platform.startswith("linux"):
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Bigdata") \
    .config("spark.sql.repl.eagerEval.enabled", "true") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

print("Spark session started ✅")


base_path = os.path.dirname(os.getcwd()) if os.path.basename(os.getcwd()) == "notebooks" else os.getcwd()
base_path = os.path.join(base_path, "data")


# Raw to bronze


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


raw_dataframes = {}
i = 0
for name, file in files.items():
    df = spark.read.csv(f"{os.path.join(base_path, "0_raw", file)}", header=True, inferSchema=True, multiLine=True, escape='"')
    raw_dataframes[name] = df
    i += 1
print(f"{i} dataframes successfully loaded ✅")


for name, df in raw_dataframes.items():
    df.write.mode("overwrite").parquet(f"{base_path}/1_bronze/{name}")
    print(f"Dataframe {name} saved to bronze ✅")


# Bronze to silver


bronze_parquets = ["orders", "customers", "order_items", "payments", "products", "sellers", "reviews", "geolocation", "translation"]

bronze_dataframes = {}
i = 0
for parquet in bronze_parquets:
    df = spark.read.parquet(f"{os.path.join(base_path, "1_bronze", parquet)}")
    bronze_dataframes[parquet] = df
    i += 1
print(f"{i} dataframes successfully loaded ✅")


bronze_dataframes["order_items"] = bronze_dataframes["order_items"] \
    .withColumn("order_item_id", bronze_dataframes["order_items"]["order_item_id"].cast(ShortType())) \
    .withColumn("price", bronze_dataframes["order_items"]["price"].cast(DecimalType(10, 2))) \
    .withColumn("freight_value", bronze_dataframes["order_items"]["freight_value"].cast(DecimalType(10, 2)))


bronze_dataframes["payments"] = bronze_dataframes["payments"] \
    .withColumn("payment_sequential", bronze_dataframes["payments"]["payment_sequential"].cast(ShortType())) \
    .withColumn("payment_installments", bronze_dataframes["payments"]["payment_installments"].cast(ShortType())) \
    .withColumn("payment_value", bronze_dataframes["payments"]["payment_value"].cast(DecimalType(10, 2)))


bronze_dataframes["products"] = bronze_dataframes["products"] \
    .withColumn("product_name_lenght", bronze_dataframes["products"]["product_name_lenght"].cast(ShortType())) \
    .withColumn("product_description_lenght", bronze_dataframes["products"]["product_description_lenght"].cast(ShortType())) \
    .withColumn("product_photos_qty", bronze_dataframes["products"]["product_photos_qty"].cast(ShortType())) \
    .withColumn("product_length_cm", bronze_dataframes["products"]["product_length_cm"].cast(ShortType())) \
    .withColumn("product_height_cm", bronze_dataframes["products"]["product_height_cm"].cast(ShortType())) \
    .withColumn("product_width_cm", bronze_dataframes["products"]["product_width_cm"].cast(ShortType()))


bronze_dataframes["reviews"] = bronze_dataframes["reviews"] \
    .withColumn("review_score", bronze_dataframes["reviews"]["review_score"].cast(ShortType()))


df_orders_approved = bronze_dataframes["orders"].filter(F.col("order_approved_at").isNotNull()) \
    .select(F.avg(F.unix_timestamp("order_approved_at") - F.unix_timestamp("order_purchase_timestamp")).alias("avg_approval_delay"))
avg_approval_delay = round(df_orders_approved.collect()[0]["avg_approval_delay"])


bronze_dataframes["orders"] = bronze_dataframes["orders"].withColumn(
    "order_approved_at", 
    F.when(
        F.col("order_approved_at").isNull() & (F.col("order_status") == "delivered"), 
        F.col("order_purchase_timestamp") + F.expr(f"INTERVAL {int(avg_approval_delay)} SECOND")
    ).otherwise(F.col("order_approved_at"))
)


bronze_dataframes["orders"] = bronze_dataframes["orders"].withColumn(
    "order_approved_at", 
    F.when(
        F.col("order_approved_at") < F.col("order_purchase_timestamp"), 
        F.col("order_purchase_timestamp") + F.expr(f"INTERVAL {int(avg_approval_delay)} SECOND")
    ).otherwise(F.col("order_approved_at"))
)


bronze_dataframes["payments"] = bronze_dataframes["payments"].withColumn(
    "payment_type", 
    F.when(F.col("payment_type") == "not_defined", "voucher").otherwise(F.col("payment_type"))
)


df_geo_unique = bronze_dataframes["geolocation"].groupBy("geolocation_zip_code_prefix").agg(
    F.avg("geolocation_lat").alias("geolocation_lat"),
    F.avg("geolocation_lng").alias("geolocation_lng"),
    F.first("geolocation_city").alias("geolocation_city"),
    F.first("geolocation_state").alias("geolocation_state")
)
bronze_dataframes["geolocation"] = df_geo_unique


df_products = bronze_dataframes["products"]
df_products_joined = df_products.join(bronze_dataframes["translation"], on="product_category_name", how="left")
bronze_dataframes["products"] = df_products_joined
del bronze_dataframes["translation"]


df_geo = bronze_dataframes["geolocation"]
df_geo = df_geo.drop("geolocation_city", "geolocation_state")


df_customers = bronze_dataframes["customers"]
df_customers_joined = df_customers.join(df_geo, on=df_customers["customer_zip_code_prefix"] == df_geo["geolocation_zip_code_prefix"], how="left")
df_customers_joined = df_customers_joined.drop("geolocation_zip_code_prefix")
bronze_dataframes["customers"] = df_customers_joined


df_sellers = bronze_dataframes["sellers"]
df_sellers_joined = df_sellers.join(df_geo, on=df_sellers["seller_zip_code_prefix"] == df_geo["geolocation_zip_code_prefix"], how="left")
df_sellers_joined = df_sellers_joined.drop("geolocation_zip_code_prefix")
bronze_dataframes["sellers"] = df_sellers_joined

del bronze_dataframes["geolocation"]


for name, df in bronze_dataframes.items():
    df.write.mode("overwrite").parquet(f"{base_path}/2_silver/{name}")
    print(f"Dataframe {name} saved to silver ✅")



# Silver to gold

silver_parquets = ["orders", "customers", "order_items", "payments", "products", "sellers", "reviews"]

silver_dataframes = {}
i = 0
for parquet in silver_parquets:
    df = spark.read.parquet(f"{os.path.join(base_path, "2_silver", parquet)}")
    silver_dataframes[parquet] = df
    i += 1
print(f"{i} dataframes successfully loaded ✅")


df_sales = silver_dataframes["orders"].filter(F.col("order_status") != "canceled") \
    .join(silver_dataframes["payments"], on="order_id", how="inner") \
    .withColumn("month_year", F.date_format("order_purchase_timestamp", "yyyy-MM")) \
    .select("order_id", "customer_id", "payment_value", "payment_type", "month_year")


df_deliveries_reviews = silver_dataframes["orders"].filter(F.col("order_status") == "delivered") \
    .withColumn("delay_days", F.datediff("order_delivered_customer_date", "order_purchase_timestamp")) \
    .withColumn("is_late", F.when(F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"), True).otherwise(False)) \
    .select("order_id", "delay_days", "is_late") \
    .join(silver_dataframes["reviews"], on="order_id", how="inner") \
    .select("order_id", "delay_days", "is_late", "review_score")


df_products_sellers = silver_dataframes["order_items"] \
    .join(silver_dataframes["products"], on="product_id", how="inner") \
    .join(silver_dataframes["sellers"], on="seller_id", how="inner") \
    .select("order_id", "product_id", "product_category_name_english", "price", "seller_id", "seller_city", "seller_state")


df_geo_customers = silver_dataframes["orders"] \
    .join(silver_dataframes["customers"], on="customer_id", how="inner") \
    .select("order_id", "customer_id", "customer_zip_code_prefix", "customer_city", "customer_state")


gold_dataframes = {
    "sales": df_sales,
    "deliveries_reviews": df_deliveries_reviews,
    "products_sellers": df_products_sellers,
    "geo_customers": df_geo_customers
}

for name, df in gold_dataframes.items():
    df.write.mode("overwrite").parquet(f"{base_path}/3_gold/{name}")
    print(f"Dataframe {name} saved to gold ✅")