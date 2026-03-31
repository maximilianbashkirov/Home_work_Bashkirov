import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct, avg,
    when, lit, coalesce, to_date, year, month, dayofmonth, weekofyear,
    row_number, desc, asc
)
from pyspark.sql.window import Window
from sqlalchemy import create_engine

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'delivery_db'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow_password')
}


def get_spark_session():
    spark = SparkSession.builder \
        .appName("DeliveryDataMart") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_orders_mart(spark):
    """Построение витрины заказов с исправленной логикой скидок"""

    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    properties = {
        "user": DB_CONFIG['user'],
        "password": DB_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }

    # Чтение данных
    orders_df = spark.read.jdbc(url=jdbc_url, table="fact_orders", properties=properties)
    order_items_df = spark.read.jdbc(url=jdbc_url, table="fact_order_items", properties=properties)
    users_df = spark.read.jdbc(url=jdbc_url, table="dim_users", properties=properties)
    addresses_df = spark.read.jdbc(url=jdbc_url, table="dim_addresses", properties=properties)
    stores_df = spark.read.jdbc(url=jdbc_url, table="dim_stores", properties=properties)
    assignments_df = spark.read.jdbc(url=jdbc_url, table="fact_delivery_assignments", properties=properties)

    # Обогащение заказов данными адресов и магазинов
    orders_with_dims = orders_df \
        .join(addresses_df, orders_df.address_id == addresses_df.address_id, "left") \
        .join(stores_df, orders_df.store_id == stores_df.store_id, "left") \
        .select(
            orders_df["*"],
            addresses_df.city.alias("address_city"),
            stores_df.store_name,
            stores_df.city.alias("store_city")
        )

    # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
    # Расчет выручки делаем через явный джойн с заказами, чтобы взять order_discount из fact_orders
    # Убираем обращение к несуществующей колонке order_discount_from_order
    
    revenue_calc = order_items_df \
        .join(
            orders_with_dims.select("order_id", "order_status", "delivered_at", "address_city", "store_id", "store_name", "order_discount"),
            on="order_id",
            how="inner"
        ) \
        .filter(col("order_status") == "delivered") \
        .withColumn("report_date", to_date(col("delivered_at"))) \
        .withColumn("item_base_total", col("quantity") * col("price")) \
        .withColumn("item_after_item_discount", 
                    col("item_base_total") * (1 - coalesce(col("item_discount"), lit(0)) / 100)) \
        .withColumn("final_revenue", 
                    col("item_after_item_discount") * (1 - coalesce(col("order_discount"), lit(0)) / 100)) \
        .groupBy("report_date", "address_city", "store_id", "store_name") \
        .agg(
            spark_sum("final_revenue").alias("revenue"),
            spark_sum("final_revenue").alias("turnover") 
        )

    # Метрики по созданным заказам
    daily_metrics = orders_with_dims \
        .filter(col("created_at").isNotNull()) \
        .withColumn("report_date", to_date(col("created_at"))) \
        .groupBy("report_date", "address_city", "store_id", "store_name") \
        .agg(
            count("*").alias("orders_created_count"),
            countDistinct("user_id").alias("unique_customers_count")
        )

    # Метрики по доставленным заказам
    delivered_metrics = orders_with_dims \
        .filter((col("delivered_at").isNotNull()) & (col("canceled_at").isNull())) \
        .withColumn("report_date", to_date(col("delivered_at"))) \
        .groupBy("report_date", "address_city", "store_id", "store_name") \
        .agg(count("*").alias("orders_delivered_count"))

    # Метрики по отмененным заказам
    canceled_metrics = orders_with_dims \
        .filter(col("canceled_at").isNotNull()) \
        .withColumn("report_date", to_date(col("canceled_at"))) \
        .groupBy("report_date", "address_city", "store_id", "store_name") \
        .agg(
            count("*").alias("orders_canceled_count"),
            spark_sum(when(col("cancellation_reason").isin("Ошибка приложения", "Проблемы с оплатой"), 1).otherwise(0))
                .alias("orders_canceled_service_error")
        )

    # Смена курьеров
    driver_changes = assignments_df \
        .join(orders_with_dims.select("order_id", "created_at", "address_city", "store_id", "store_name"), "order_id") \
        .withColumn("report_date", to_date(col("created_at"))) \
        .groupBy("order_id", "report_date", "address_city", "store_id", "store_name") \
        .agg(count("*").alias("driver_count")) \
        .filter(col("driver_count") > 1) \
        .groupBy("report_date", "address_city", "store_id", "store_name") \
        .agg(count("*").alias("driver_change_count"))

    # Активные курьеры
    active_drivers = assignments_df \
        .join(orders_with_dims.select("order_id", "created_at", "address_city", "store_id", "store_name"), "order_id") \
        .withColumn("report_date", to_date(col("created_at"))) \
        .groupBy("report_date", "address_city", "store_id", "store_name") \
        .agg(countDistinct("driver_id").alias("active_drivers_count"))

    # Сборка финального датафрейма
    mart_df = daily_metrics \
        .join(delivered_metrics, ["report_date", "address_city", "store_id", "store_name"], "left") \
        .join(canceled_metrics, ["report_date", "address_city", "store_id", "store_name"], "left") \
        .join(revenue_calc, ["report_date", "address_city", "store_id", "store_name"], "left") \
        .join(driver_changes, ["report_date", "address_city", "store_id", "store_name"], "left") \
        .join(active_drivers, ["report_date", "address_city", "store_id", "store_name"], "left") \
        .withColumn("year", year(col("report_date"))) \
        .withColumn("month", month(col("report_date"))) \
        .withColumn("day", dayofmonth(col("report_date"))) \
        .withColumn("profit", coalesce(col("revenue"), lit(0)) * 0.2) \
        .withColumn("orders_canceled_after_delivery", lit(0)) \
        .withColumn("avg_check",
                    when(col("orders_delivered_count") > 0,
                         coalesce(col("revenue"), lit(0)) / col("orders_delivered_count"))
                    .otherwise(lit(0))) \
        .withColumn("orders_per_customer",
                    when(col("unique_customers_count") > 0,
                         coalesce(col("orders_created_count"), lit(0)) / col("unique_customers_count"))
                    .otherwise(lit(0))) \
        .withColumn("revenue_per_customer",
                    when(col("unique_customers_count") > 0,
                         coalesce(col("revenue"), lit(0)) / col("unique_customers_count"))
                    .otherwise(lit(0))) \
        .na.fill({
            "orders_delivered_count": 0,
            "orders_canceled_count": 0,
            "orders_canceled_service_error": 0,
            "revenue": 0,
            "turnover": 0,
            "profit": 0,
            "driver_change_count": 0,
            "active_drivers_count": 0
        }) \
        .select(
            "report_date", "year", "month", "day",
            col("address_city").alias("city"),
            "store_id", "store_name",
            "turnover", "revenue", "profit",
            "orders_created_count", "orders_delivered_count", "orders_canceled_count",
            "orders_canceled_after_delivery", "orders_canceled_service_error",
            "unique_customers_count", "avg_check", "orders_per_customer", "revenue_per_customer",
            "driver_change_count", "active_drivers_count"
        )

    return mart_df


def build_products_mart(spark):
    """Построение витрины товаров"""

    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    properties = {
        "user": DB_CONFIG['user'],
        "password": DB_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }

    orders_df = spark.read.jdbc(url=jdbc_url, table="fact_orders", properties=properties)
    order_items_df = spark.read.jdbc(url=jdbc_url, table="fact_order_items", properties=properties)
    items_df = spark.read.jdbc(url=jdbc_url, table="dim_items", properties=properties)
    categories_df = spark.read.jdbc(url=jdbc_url, table="dim_categories", properties=properties)
    addresses_df = spark.read.jdbc(url=jdbc_url, table="dim_addresses", properties=properties)
    stores_df = spark.read.jdbc(url=jdbc_url, table="dim_stores", properties=properties)

    orders_with_address = orders_df \
        .join(addresses_df, orders_df.address_id == addresses_df.address_id, "left") \
        .select(orders_df["*"], addresses_df.city.alias("city"))

    order_items_full = order_items_df \
        .join(orders_with_address, "order_id") \
        .join(items_df, order_items_df.item_id == items_df.item_id, "left") \
        .join(categories_df, items_df.category_id == categories_df.category_id, "left") \
        .select(
            order_items_df["*"],
            orders_with_address.city,
            orders_with_address.store_id,
            orders_with_address.created_at,
            orders_with_address.delivered_at,
            orders_with_address.canceled_at,
            orders_with_address.order_status,
            items_df.item_title,
            items_df.category_id.alias("category_id"),
            categories_df.category_name
        )

    # МЕТРИКИ
    product_metrics = order_items_full \
        .filter(col("order_status") == "delivered") \
        .withColumn("report_date", to_date(coalesce(col("delivered_at"), col("created_at")))) \
        .withColumn("item_turnover",
                    col("quantity") * col("price") *
                    (1 - coalesce(col("item_discount"), lit(0)) / 100)) \
        .groupBy(
            "report_date", "city", "store_id", "category_id", "category_name", "item_id", "item_title"
        ) \
        .agg(
            spark_sum("item_turnover").alias("item_turnover"),
            spark_sum("quantity").alias("ordered_quantity"),
            spark_sum("canceled_quantity").alias("canceled_quantity"),
            countDistinct("order_id").alias("orders_with_item_count"),
            countDistinct(when(col("canceled_quantity") > 0, col("order_id"))).alias("orders_with_cancel_count")
        )

    product_metrics = product_metrics \
        .withColumn("year", year(col("report_date"))) \
        .withColumn("month", month(col("report_date"))) \
        .withColumn("day", dayofmonth(col("report_date"))) \
        .withColumn("week", weekofyear(col("report_date")))

    window_day = Window.partitionBy("report_date", "city", "store_id").orderBy(desc("ordered_quantity"))
    window_day_asc = Window.partitionBy("report_date", "city", "store_id").orderBy(asc("ordered_quantity"))

    product_metrics = product_metrics \
        .withColumn("rn_day", row_number().over(window_day)) \
        .withColumn("rn_day_asc", row_number().over(window_day_asc)) \
        .withColumn("is_most_popular_day", col("rn_day") == 1) \
        .withColumn("is_least_popular_day", col("rn_day_asc") == 1)

    window_week = Window.partitionBy("year", "week", "city", "store_id").orderBy(desc("ordered_quantity"))
    window_week_asc = Window.partitionBy("year", "week", "city", "store_id").orderBy(asc("ordered_quantity"))

    product_metrics = product_metrics \
        .withColumn("rn_week", row_number().over(window_week)) \
        .withColumn("rn_week_asc", row_number().over(window_week_asc)) \
        .withColumn("is_most_popular_week", col("rn_week") == 1) \
        .withColumn("is_least_popular_week", col("rn_week_asc") == 1)

    window_month = Window.partitionBy("year", "month", "city", "store_id").orderBy(desc("ordered_quantity"))
    window_month_asc = Window.partitionBy("year", "month", "city", "store_id").orderBy(asc("ordered_quantity"))

    product_metrics = product_metrics \
        .withColumn("rn_month", row_number().over(window_month)) \
        .withColumn("rn_month_asc", row_number().over(window_month_asc)) \
        .withColumn("is_most_popular_month", col("rn_month") == 1) \
        .withColumn("is_least_popular_month", col("rn_month_asc") == 1) \
        .drop("rn_day", "rn_day_asc", "rn_week", "rn_week_asc", "rn_month", "rn_month_asc")

    product_metrics = product_metrics.na.fill({
        "item_turnover": 0,
        "ordered_quantity": 0,
        "canceled_quantity": 0,
        "orders_with_item_count": 0,
        "orders_with_cancel_count": 0
    })

    return product_metrics


def write_mart_to_postgres(df, table_name, mode="overwrite"):
    """Запись витрины в PostgreSQL"""
    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    properties = {
        "user": DB_CONFIG['user'],
        "password": DB_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=properties
    )


def transform_and_build_marts(**kwargs):
    """Основная функция запуска трансформации"""
    print("построениЕ витрин")

    spark = get_spark_session()

    try:
        # Построение витрины заказов
        print("Построение витрины заказов...")
        orders_mart = build_orders_mart(spark)
        orders_mart.show(truncate=False)

        write_mart_to_postgres(orders_mart, "mart_orders", mode="overwrite")
        print(f"Витрина заказов построена. Записей: {orders_mart.count()}")

        print("Построение витрины товаров...")
        products_mart = build_products_mart(spark)
        products_mart.show(truncate=False)

        write_mart_to_postgres(products_mart, "mart_products", mode="overwrite")
        print(f"Витрина товаров построена. Записей: {products_mart.count()}")

        print("Процесс трансформации и построения витрин завершен успешно!")

    finally:
        spark.stop()

    return {'status': 'success'}