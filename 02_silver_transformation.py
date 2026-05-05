# Silver Layer - Cleaning and joining

from pyspark.sql.functions import col, to_date, explode

customer_df = spark.read.format("delta").load("/mnt/motor/bronze/customer")
vehicle_df = spark.read.format("delta").load("/mnt/motor/bronze/vehicle")
dealer_df = spark.read.format("delta").load("/mnt/motor/bronze/dealership")
sales_df = spark.read.format("delta").load("/mnt/motor/bronze/sales")
service_df = spark.read.format("delta").load("/mnt/motor/bronze/service")
parts_df = spark.read.format("delta").load("/mnt/motor/bronze/spare_parts")
feedback_df = spark.read.format("delta").load("/mnt/motor/bronze/service_feedback")
loyalty_raw_df = spark.read.format("delta").load("/mnt/motor/bronze/customer_loyalty_raw")

loyalty_df = loyalty_raw_df.select(explode(col("data")).alias("data")).select(
    col("data.customer_id").alias("customer_id"),
    col("data.loyalty_score").alias("loyalty_score"),
    col("data.segment").alias("segment")
)

sales_df = sales_df.withColumn("sale_date", to_date("sale_date")) \
                   .withColumn("sale_price", col("sale_price").cast("int"))

service_df = service_df.withColumn("service_date", to_date("service_date")) \
                       .withColumn("service_cost", col("service_cost").cast("int"))

vehicle_df = vehicle_df.withColumn("price", col("price").cast("int"))
parts_df = parts_df.withColumn("part_cost", col("part_cost").cast("int"))

silver_sales = sales_df.join(customer_df, "customer_id", "left") \
                       .join(vehicle_df, "vehicle_id", "left") \
                       .join(dealer_df, "dealer_id", "left") \
                       .join(loyalty_df, "customer_id", "left")

silver_service = service_df.join(customer_df, "customer_id", "left") \
                           .join(vehicle_df, "vehicle_id", "left") \
                           .join(feedback_df, "service_id", "left")

silver_sales.write.format("delta").mode("overwrite").save("/mnt/motor/silver/sales_full")
silver_service.write.format("delta").mode("overwrite").save("/mnt/motor/silver/service_full")
parts_df.write.format("delta").mode("overwrite").save("/mnt/motor/silver/spare_parts")
