import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Olist-Silver") \
    .master("local[*]") \
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

orders = spark.read.parquet(f"{base_path}/data/1_bronze/orders")
payments = spark.read.parquet(f"{base_path}/data/1_bronze/payments")
reviews = spark.read.parquet(f"{base_path}/data/1_bronze/reviews")

print(f"orders : {orders.count()} rows")
print(f"payments : {payments.count()} rows")
print(f"reviews : {reviews.count()} rows")


for name, df in [("orders", orders), ("payments", payments), ("reviews", reviews)]:
    print(f"\n{name} :")
    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            print(f"  {col} : {null_count} nulls")