from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Generated ETL Job").getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://db.example.local:5432/mydb") \
    .option("dbtable", "public.users") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .load()



df = df.filter("age &gt; 25")





df = df.select( "id" ,  "name" ,  "age" )



df.write.mode("overwrite").parquet("s3://my-bucket/output/users/")
