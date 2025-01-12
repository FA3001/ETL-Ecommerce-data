from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# PostgreSQL connection details
BRONZE_DB = "Bronze"
SILVER_DB = "Silver"
POSTGRES_HOST = "172.18.0.4"
POSTGRES_PORT = "5432"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
JDBC_DRIVER = "/usr/local/airflow/plugins/postgresql-42.4.4.jar"  # Adjust the JDBC driver path

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Silver Layer Processing") \
    .config("spark.jars", JDBC_DRIVER) \
    .config("spark.driver.extraClassPath", JDBC_DRIVER) \
    .getOrCreate()

# Function to read from Bronze
def read_from_bronze(table_name):
    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{BRONZE_DB}") \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()

# Function to write to Silver
def write_to_silver(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{SILVER_DB}") \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

try:
    # Process Orders
    orders_df = read_from_bronze("orders")
    silver_orders = orders_df \
        .withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp")) \
        .withColumn("order_approved_at", to_timestamp("order_approved_at")) \
        .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date")) \
        .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date")) \
        .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date")) \
        .dropDuplicates() \
        .na.fill("pending", ["order_status"])
    write_to_silver(silver_orders, "orders")

    # Process Customers
    customers_df = read_from_bronze("customers")
    silver_customers = customers_df \
        .dropDuplicates(["customer_id"]) \
        .na.drop() \
        .withColumn("customer_city", lower(trim(col("customer_city")))) \
        .withColumn("customer_state", upper(trim(col("customer_state"))))
    write_to_silver(silver_customers, "customers")

    # Process Order Items with Price Validation
    order_items_df = read_from_bronze("order_items")
    silver_order_items = order_items_df \
        .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date")) \
        .withColumn("price", col("price").cast(DecimalType(10, 2))) \
        .withColumn("freight_value", col("freight_value").cast(DecimalType(10, 2))) \
        .filter(col("price") > 0) \
        .filter(col("freight_value") >= 0)
    write_to_silver(silver_order_items, "order_items")

    # Process Products with Category Translation
    products_df = read_from_bronze("products")
    category_translation_df = read_from_bronze("product_category_translation")
    
    silver_products = products_df \
        .join(category_translation_df, "product_category_name", "left") \
        .withColumn("product_category_name_english", 
                    coalesce(col("product_category_name_english"), col("product_category_name"))) \
        .dropDuplicates(["product_id"]) \
        .na.fill("unknown", ["product_category_name_english"])
    write_to_silver(silver_products, "products")

    # Process Sellers
    sellers_df = read_from_bronze("sellers")
    silver_sellers = sellers_df \
        .dropDuplicates(["seller_id"]) \
        .withColumn("seller_city", lower(trim(col("seller_city")))) \
        .withColumn("seller_state", upper(trim(col("seller_state"))))
    write_to_silver(silver_sellers, "sellers")

    # Process Order Reviews
    reviews_df = read_from_bronze("order_reviews")
    silver_reviews = reviews_df \
        .withColumn("review_creation_date", to_timestamp("review_creation_date")) \
        .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp")) \
        .dropDuplicates(["review_id"]) \
        .na.fill(0, ["review_score"])
    write_to_silver(silver_reviews, "reviews")

except Exception as e:
    print(f"Error in Silver layer processing: {e}")
finally:
    spark.stop()