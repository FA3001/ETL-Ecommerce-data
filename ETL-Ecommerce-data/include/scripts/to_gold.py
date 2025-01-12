from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import to_timestamp, date_format


# PostgreSQL connection details
SILVER_DB = "Silver"
GOLD_DB = "Gold"
POSTGRES_HOST = "172.18.0.4"
POSTGRES_PORT = "5432"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
JDBC_DRIVER = "/usr/local/airflow/plugins/postgresql-42.4.4.jar"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Gold Layer Processing") \
    .config("spark.jars", JDBC_DRIVER) \
    .config("spark.driver.extraClassPath", JDBC_DRIVER) \
    .getOrCreate()

# Function to read from Silver
def read_from_silver(table_name):
    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{SILVER_DB}") \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()

# Function to write to Gold
def write_to_gold(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{GOLD_DB}") \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

try:
    # Read necessary tables from Silver
    orders_df = read_from_silver("orders")
    customers_df = read_from_silver("customers")
    order_items_df = read_from_silver("order_items")
    products_df = read_from_silver("products")
    sellers_df = read_from_silver("sellers")
    reviews_df = read_from_silver("reviews")

    # 1. Daily Sales Analytics
    daily_sales = order_items_df.join(orders_df, "order_id") \
        .withColumn("date", to_date("order_purchase_timestamp")) \
        .groupBy("date") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("price").alias("total_revenue"),
            avg("price").alias("average_order_value"),
            sum("freight_value").alias("total_freight_value")
        )
    write_to_gold(daily_sales, "daily_sales")
    
        # 2. Customer Metrics (with formatted dates)
    customer_metrics = order_items_df.join(orders_df, "order_id") \
        .join(customers_df, "customer_id") \
        .withColumn("first_purchase_date", date_format(min("order_purchase_timestamp").over(Window.partitionBy("customer_id")), "yyyy-MM-dd")) \
        .withColumn("last_purchase_date", date_format(max("order_purchase_timestamp").over(Window.partitionBy("customer_id")), "yyyy-MM-dd")) \
        .groupBy("customer_id", "customer_state", "first_purchase_date", "last_purchase_date") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("price").alias("total_spent"),
            avg("price").alias("avg_order_value"),
            datediff(max("order_purchase_timestamp"), 
                    min("order_purchase_timestamp")).alias("customer_lifetime_days")
        )
    write_to_gold(customer_metrics, "customer_metrics")
    
    # 3. Product Performance
    product_performance = order_items_df.join(products_df, "product_id") \
        .groupBy("product_id", "product_category_name_english") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("price").alias("total_revenue"),
            avg("price").alias("avg_price"),
            sum("freight_value").alias("total_freight_cost")
        )
    write_to_gold(product_performance, "product_performance")
  # 4. Seller Performance
    seller_performance = order_items_df.join(sellers_df, "seller_id") \
        .join(orders_df, "order_id") \
        .withColumn("order_purchase_date", date_format(to_timestamp("order_purchase_timestamp"), "yyyy-MM-dd")) \
        .withColumn("order_delivered_date", date_format(to_timestamp("order_delivered_customer_date"), "yyyy-MM-dd")) \
        .groupBy("seller_id", "seller_state") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("price").alias("total_revenue"),
            avg("price").alias("avg_order_value"),
            avg(datediff(col("order_delivered_customer_date"), 
                        col("order_purchase_timestamp"))).alias("avg_delivery_time")
        )
    write_to_gold(seller_performance, "seller_performance")

    # 5. Customer Satisfaction Metrics
    satisfaction_metrics = reviews_df.join(orders_df, "order_id") \
        .withColumn("review_date", date_format(to_timestamp("review_creation_date"), "yyyy-MM-dd")) \
        .groupBy("order_id", "review_date") \
        .agg(
            avg("review_score").alias("avg_review_score"),
            count("review_id").alias("review_count")
        )
    write_to_gold(satisfaction_metrics, "satisfaction_metrics")

    # 6. Delivery Performance
    delivery_performance = orders_df \
        .withColumn("order_purchase_date", date_format(to_timestamp("order_purchase_timestamp"), "yyyy-MM-dd")) \
        .withColumn("order_delivered_date", date_format(to_timestamp("order_delivered_customer_date"), "yyyy-MM-dd")) \
        .withColumn("estimated_delivery_date", date_format(to_timestamp("order_estimated_delivery_date"), "yyyy-MM-dd")) \
        .withColumn("delivery_delay", 
                   datediff(col("order_delivered_customer_date"), 
                           col("order_estimated_delivery_date"))) \
        .groupBy("order_status") \
        .agg(
            count("order_id").alias("total_orders"),
            avg("delivery_delay").alias("avg_delivery_delay"),
            sum(when(col("delivery_delay") > 0, 1).otherwise(0)).alias("delayed_orders")
        )
    write_to_gold(delivery_performance, "delivery_performance")

except Exception as e:
    print(f"Error in Gold layer processing: {e}")
finally:
    spark.stop()