# Gold Layer - Business reporting tables

from pyspark.sql.functions import sum, count, avg, month, year

sales_df = spark.read.format("delta").load("/mnt/motor/silver/sales_full")
service_df = spark.read.format("delta").load("/mnt/motor/silver/service_full")
parts_df = spark.read.format("delta").load("/mnt/motor/silver/spare_parts")

gold_sales_by_city = sales_df.groupBy("city").agg(
    count("sale_id").alias("total_vehicles_sold"),
    sum("sale_price").alias("total_sales_revenue")
)

gold_model_sales = sales_df.groupBy("model", "fuel_type").agg(
    count("sale_id").alias("sales_count"),
    sum("sale_price").alias("sales_revenue")
)

gold_service_revenue = service_df.groupBy("service_type").agg(
    count("service_id").alias("total_services"),
    sum("service_cost").alias("total_service_revenue"),
    avg("rating").alias("avg_service_rating")
)

gold_monthly_sales = sales_df.withColumn("sales_year", year("sale_date")) \
    .withColumn("sales_month", month("sale_date")) \
    .groupBy("sales_year", "sales_month") \
    .agg(
        count("sale_id").alias("total_sales"),
        sum("sale_price").alias("total_revenue")
    )

gold_customer_360 = sales_df.join(
    service_df.select("customer_id", "service_id", "service_type", "service_cost", "rating"),
    "customer_id",
    "left"
)

gold_sales_by_city.write.format("delta").mode("overwrite").save("/mnt/motor/gold/sales_by_city")
gold_model_sales.write.format("delta").mode("overwrite").save("/mnt/motor/gold/model_sales")
gold_service_revenue.write.format("delta").mode("overwrite").save("/mnt/motor/gold/service_revenue")
gold_monthly_sales.write.format("delta").mode("overwrite").save("/mnt/motor/gold/monthly_sales")
gold_customer_360.write.format("delta").mode("overwrite").save("/mnt/motor/gold/customer_360")
