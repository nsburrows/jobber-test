import pyspark
from pyspark.sql.functions import to_date, round, avg, year, month
import time

# Track time taken to run the script

# Some Prerequisites:
#   Used JDK 17: https://adoptium.net/en-GB/temurin/releases/?version=17
#   Used Spark and Delta Lake 4.0.0 to ensure compatibility with each other
#   Used Python 3.13
#   Got compatible JARs that you will see retrieved when starting the Spark session

print("Starting the Jobber Data Pipeline...")
start_time = time.time()
# Jobber Data Pipeline
# This script reads customer and sales data, cleans it, enhances it, and saves it as a Delta table.


# Step 0: Initialize Spark Session with Delta Lake support
spark = (
    pyspark.sql.SparkSession.builder
    .appName("JobberDataPipeline")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", 8)
    .getOrCreate()
)

# Step 1: Read the data
customer_df = spark.read.parquet('./data/customer_data.parquet')
sales_df = spark.read.option("header", "true").csv("./data/sales_data.csv")


# Sanity Check: Take a look at the data we just read to see if it looks correct
print(f"Number of customers: {customer_df.count()}")
print(f"Number of sales: {sales_df.count()}")

# Step 2: Clean up the data
# We want to remove any rows where customer_id is null
# same for sales we want to remove any rows where invoice_no is null
customer_df = customer_df.dropna(subset=['customer_id'])
sales_df = sales_df.dropna(subset=['invoice_no'])

# Remove any duplicates
customer_df = customer_df.dropDuplicates(['customer_id'])
sales_df = sales_df.dropDuplicates(['invoice_no'])

# We can make some inferred changes to do the data types based on the samples earlier:
sales_df = sales_df.withColumn("quantity", sales_df["quantity"].cast("int")) \
                   .withColumn("price", sales_df["price"].cast("float")) \
                   .withColumn("invoice_date", to_date(sales_df["invoice_date"], "dd-MM-yyyy"))



# For Customer data fill in empty age records with the average age
avg_age = customer_df.select(avg("age")).first()[0]
customer_df = customer_df.fillna({"age": avg_age})

# Step 3: Add some enhancements to the data
# Since there is no total we can also create a total_price column
# Typically sales/invoice data is looked at for time periods so it would be sensible to add year and month so they can
# be used for partitioning later on
sales_df = (
    sales_df
            .withColumn("total_price", sales_df["quantity"] * sales_df["price"])
            .withColumn("year", year(sales_df["invoice_date"]))
            .withColumn("month", month(sales_df["invoice_date"]))
)

# total_price is a accounting field, so we can round it to 2 decimal places
sales_df = sales_df.withColumn("total_price", round(sales_df["total_price"], 2))

# Since this is a one to one mapping we can join the two tables together
# Assuming that a customer can technically not have any sales and also considering that a customer can have multiple
# sales, we will do a left join
full_sales_df = customer_df.join(sales_df, on="customer_id", how="left")

# Step 4: Let's save the cleaned and joined data as a Delta table
full_sales_df.write.format("delta").partitionBy("year", "month").mode("overwrite").option("overwriteSchema", "true").save("./data/full_sales_data.delta")

# Sanity Check: Read the Delta table back to verify
delta_df = spark.read.format("delta").load("./data/full_sales_data.delta")
print(f"Number of records in Delta table: {delta_df.count()}")

end_time = time.time()
time_taken = end_time - start_time
print("Finished processing the Jobber Data Pipeline in {:.2f} seconds.".format(time_taken))