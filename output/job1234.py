from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("Generated ETL Job").getOrCreate()

# Define source data loading

# Load data from S3-compatible storage
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIABBDYWBVDYWB")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "HBYHBCYEFHBDEUBDWUSWUDBWD")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://s3-penyedia-layanan.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
df = spark.read.format("parquet").load("s3a://ramyeon/price.csv")


# Apply transforms

# Transform 0: filter

df = df.filter("status_code &gt;= 400")



# Transform 1: add_column

df = df.withColumn("is_error", F.expr("status_code &gt;= 500"))



# Transform 2: drop_column

df = df.drop("request_body", "response_body")




# Write to target destination

# Write to S3-compatible storage
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIABBDYWBVDYWB")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "HBYHBCYEFHBDEUBDWUSWUDBWD")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http:/s3-penyedia-layanan.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
df.write.mode("overwrite").parquet("s3a://cleaned-ramyeon/price.csv")


print("ETL job completed successfully!")