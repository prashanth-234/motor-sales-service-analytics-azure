# Bronze Layer - Raw ingestion

customer_df = spark.read.option("header", True).csv("/mnt/motor/raw/sqlserver/customer/")
vehicle_df = spark.read.option("header", True).csv("/mnt/motor/raw/sqlserver/vehicle/")
dealer_df = spark.read.option("header", True).csv("/mnt/motor/raw/sqlserver/dealership/")

sales_df = spark.read.option("header", True).csv("/mnt/motor/raw/blob/sales/")
service_df = spark.read.option("header", True).csv("/mnt/motor/raw/blob/service/")
parts_df = spark.read.option("header", True).csv("/mnt/motor/raw/blob/spare_parts/")

feedback_df = spark.read.option("multiline", True).json("/mnt/motor/raw/cosmos/service_feedback/")
loyalty_raw_df = spark.read.option("multiline", True).json("/mnt/motor/raw/api/customer_loyalty/")

customer_df.write.format("delta").mode("overwrite").save("/mnt/motor/bronze/customer")
vehicle_df.write.format("delta").mode("overwrite").save("/mnt/motor/bronze/vehicle")
dealer_df.write.format("delta").mode("overwrite").save("/mnt/motor/bronze/dealership")
sales_df.write.format("delta").mode("overwrite").save("/mnt/motor/bronze/sales")
service_df.write.format("delta").mode("overwrite").save("/mnt/motor/bronze/service")
parts_df.write.format("delta").mode("overwrite").save("/mnt/motor/bronze/spare_parts")
feedback_df.write.format("delta").mode("overwrite").save("/mnt/motor/bronze/service_feedback")
loyalty_raw_df.write.format("delta").mode("overwrite").save("/mnt/motor/bronze/customer_loyalty_raw")
