from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# PostgreSQL connection details
POSTGRES_HOST = "172.18.0.4"  # Replace with your PostgreSQL container hostname
POSTGRES_PORT = "5432"
POSTGRES_DB = "Bronze"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_DRIVER = "/usr/local/airflow/plugins/postgresql-42.4.4.jar"  # Adjust the JDBC driver path

# Dataset directory inside the container
DATASET_DIR = "./include"

# List of files and their corresponding table names
FILE_TABLE_MAPPING = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_translation"
}

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Olist Dataset to PostgreSQL") \
    .config("spark.jars", JDBC_DRIVER) \
    .config("spark.driver.extraClassPath", JDBC_DRIVER) \
    .getOrCreate()

# Function to load CSV and write to PostgreSQL
def load_to_postgres(file_path, table_name):
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        
        df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", table_name) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .option("stringtype", "unspecified") \
            .mode("overwrite") \
            .save()
        print(f"Successfully loaded {file_path} into {table_name} table.")
    except Exception as e:
        print(f"Error loading {file_path} into {table_name} table: {e}")

# Iterate through the dataset files and upload to PostgreSQL
for file_name, table_name in FILE_TABLE_MAPPING.items():
    file_path = f"{DATASET_DIR}/{file_name}"
    load_to_postgres(file_path, table_name)

# Stop Spark session
spark.stop()
