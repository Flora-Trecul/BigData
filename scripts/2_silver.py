import os
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, ShortType
from pyspark.sql import functions as F

# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Bigdata") \
    .config("spark.sql.repl.eagerEval.enabled", "true") \
    .getOrCreate()

print("Spark session started ✅")


base_path = os.path.dirname(os.getcwd()) if os.path.basename(os.getcwd()) == "notebooks" else os.getcwd()
base_path = os.path.join(base_path, "data")

parquets = ["orders", "customers", "order_items", "payments", "products", "sellers", "reviews", "geolocation", "translation"]

dataframes = {}
i = 0
for parquet in parquets:
    df = spark.read.parquet(f"{os.path.join(base_path, "1_bronze", parquet)}")
    dataframes[parquet] = df
    i += 1
print(f"{i} dataframes successfully loaded ✅")


# Types

for name, df in dataframes.items():
    print(name.upper())
    df.printSchema()


# Conversion of prices to DecimalType for better precision and conversion of small integers to ShortType

dataframes["order_items"] = dataframes["order_items"] \
    .withColumn("order_item_id", dataframes["order_items"]["order_item_id"].cast(ShortType())) \
    .withColumn("price", dataframes["order_items"]["price"].cast(DecimalType(10, 2))) \
    .withColumn("freight_value", dataframes["order_items"]["freight_value"].cast(DecimalType(10, 2)))


dataframes["payments"] = dataframes["payments"] \
    .withColumn("payment_sequential", dataframes["payments"]["payment_sequential"].cast(ShortType())) \
    .withColumn("payment_installments", dataframes["payments"]["payment_installments"].cast(ShortType())) \
    .withColumn("payment_value", dataframes["payments"]["payment_value"].cast(DecimalType(10, 2)))


dataframes["products"] = dataframes["products"] \
    .withColumn("product_name_lenght", dataframes["products"]["product_name_lenght"].cast(ShortType())) \
    .withColumn("product_description_lenght", dataframes["products"]["product_description_lenght"].cast(ShortType())) \
    .withColumn("product_photos_qty", dataframes["products"]["product_photos_qty"].cast(ShortType())) \
    .withColumn("product_length_cm", dataframes["products"]["product_length_cm"].cast(ShortType())) \
    .withColumn("product_height_cm", dataframes["products"]["product_height_cm"].cast(ShortType())) \
    .withColumn("product_width_cm", dataframes["products"]["product_width_cm"].cast(ShortType()))


dataframes["reviews"] = dataframes["reviews"].withColumn("review_score", dataframes["reviews"]["review_score"].cast(ShortType()))


# Null values

for name, df in dataframes.items():
    print(f"\n{name.upper()} :")
    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            print(f"  {col} : {null_count} nulls")

print("For the reviews dataframe, the null values are only reviews without comment, only a grade.")


# Handling null values for orders

print("Null values in the orders dataframe, by order status:")

dataframes["orders"].groupBy("order_status").agg(
    F.sum(F.when(F.col("order_approved_at").isNull(), 1).otherwise(0)).alias("null_approved"),
    F.sum(F.when(F.col("order_delivered_carrier_date").isNull(), 1).otherwise(0)).alias("null_carrier"),
    F.sum(F.when(F.col("order_delivered_customer_date").isNull(), 1).otherwise(0)).alias("null_customer")
).orderBy("order_status").show()

delivered_not_approved = dataframes["orders"].filter((F.col("order_status") == "delivered") & (F.col("order_approved_at").isNull()))
print(f"The {delivered_not_approved.count()} delivered orders without approval date are illogical, we replace them with the average approval delay.")


df_orders_approved = dataframes["orders"].filter(F.col("order_approved_at").isNotNull()) \
    .select(F.avg(F.unix_timestamp("order_approved_at") - F.unix_timestamp("order_purchase_timestamp")).alias("avg_approval_delay"))
avg_approval_delay = round(df_orders_approved.collect()[0]["avg_approval_delay"])


dataframes["orders"] = dataframes["orders"].withColumn(
    "order_approved_at", 
    F.when(
        F.col("order_approved_at").isNull() & (F.col("order_status") == "delivered"), 
        F.col("order_purchase_timestamp") + F.expr(f"INTERVAL {int(avg_approval_delay)} SECOND")
    ).otherwise(F.col("order_approved_at"))
)

print("We apply the same logic for the orders where the approval time is before the purchase time.")
dataframes["orders"] = dataframes["orders"].withColumn(
    "order_approved_at", 
    F.when(
        F.col("order_approved_at") < F.col("order_purchase_timestamp"), 
        F.col("order_purchase_timestamp") + F.expr(f"INTERVAL {int(avg_approval_delay)} SECOND")
    ).otherwise(F.col("order_approved_at"))
)


# Inconsistencies

print("Null or negative payments:")
dataframes["payments"].filter(F.col("payment_value") == 0).show()

print("These payments are vouchers or probably reductions so we don't need to handle them.")
print("However, we replace the not_defined payment types with voucher.")

dataframes["payments"] = dataframes["payments"].withColumn(
    "payment_type", 
    F.when(F.col("payment_type") == "not_defined", "voucher").otherwise(F.col("payment_type"))
)

print("Other inconsistencies for the orders dataframe:")
print(f"   Delivered orders without customer delivery date: {dataframes["orders"].filter((F.col('order_status') == 'delivered') & F.col('order_delivered_customer_date').isNull()).count()}")
print(f"   Delivered orders without carrier date : {dataframes["orders"].filter((F.col('order_status') == 'delivered') & F.col('order_delivered_carrier_date').isNull()).count()}")


# Duplicates

for name, df in dataframes.items():
    print(f"{name.upper()} : {df.count() - df.distinct().count()} duplicates")

from pyspark.sql import functions as F

geo_duplicates = dataframes["geolocation"].groupBy("geolocation_zip_code_prefix").count().filter("count > 1")
print("GEOLOCATION - Duplicated zip codes")
geo_duplicates.show(5)


df_geo_unique = dataframes["geolocation"].groupBy("geolocation_zip_code_prefix").agg(
    F.avg("geolocation_lat").alias("geolocation_lat"),
    F.avg("geolocation_lng").alias("geolocation_lng"),
    F.first("geolocation_city").alias("geolocation_city"),
    F.first("geolocation_state").alias("geolocation_state")
)

print("\n\nGEOLOCATION before removing duplicates -", dataframes["geolocation"].count(), "lines")
dataframes["geolocation"] = df_geo_unique
print("GEOLOCATION after removing duplicates -", dataframes["geolocation"].count(), "lines")
dataframes["geolocation"].show(5)


# Basic joins

df_products = dataframes["products"]
df_products_joined = df_products.join(dataframes["translation"], on="product_category_name", how="left")
dataframes["products"] = df_products_joined
del dataframes["translation"]


df_geo = dataframes["geolocation"]
df_geo = df_geo.drop("geolocation_city", "geolocation_state")

df_customers = dataframes["customers"]
df_customers_joined = df_customers.join(df_geo, on=df_customers["customer_zip_code_prefix"] == df_geo["geolocation_zip_code_prefix"], how="left")
df_customers_joined = df_customers_joined.drop("geolocation_zip_code_prefix")
dataframes["customers"] = df_customers_joined

df_sellers = dataframes["sellers"]
df_sellers_joined = df_sellers.join(df_geo, on=df_sellers["seller_zip_code_prefix"] == df_geo["geolocation_zip_code_prefix"], how="left")
df_sellers_joined = df_sellers_joined.drop("geolocation_zip_code_prefix")
dataframes["sellers"] = df_sellers_joined

del dataframes["geolocation"]


# Exports

for name, df in dataframes.items():
    df.write.mode("overwrite").parquet(f"{base_path}/2_silver/{name}")
    print(f"Dataframe {name} saved to silver ✅")