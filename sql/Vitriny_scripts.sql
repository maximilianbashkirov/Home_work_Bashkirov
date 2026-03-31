--Общая сводка по сети--
SELECT 
    COUNT(*) AS total_orders,
    ROUND(SUM(revenue)::NUMERIC, 2) AS total_revenue,
    ROUND(SUM(profit)::NUMERIC, 2) AS total_profit,
    ROUND(AVG(avg_check)::NUMERIC, 2) AS avg_check_network,
    SUM(orders_delivered_count) AS delivered_count,
    SUM(orders_canceled_count) AS canceled_count,
    ROUND((SUM(orders_canceled_count)::NUMERIC / NULLIF(SUM(orders_created_count), 0) * 100), 2) AS cancel_rate_percent
FROM mart_orders;

--Динамика выручки и прибыли (по дням)--
SELECT 
    report_date,
    store_name,
    city,
    ROUND(revenue::NUMERIC, 2) AS revenue,
    ROUND(profit::NUMERIC, 2) AS profit,
    orders_delivered_count,
    orders_canceled_count
FROM mart_orders
ORDER BY report_date DESC, revenue DESC
LIMIT 100;

--Топ-10 магазинов по прибыли за всё время--
SELECT 
    store_name,
    city,
    COUNT(DISTINCT report_date) AS active_days,
    SUM(orders_delivered_count) AS total_delivered,
    ROUND(SUM(revenue)::NUMERIC, 2) AS total_revenue,
    ROUND(SUM(profit)::NUMERIC, 2) AS total_profit,
    ROUND((SUM(profit)::NUMERIC / NULLIF(SUM(revenue), 0) * 100), 2) AS profit_margin_percent
FROM mart_orders
GROUP BY store_name, city
ORDER BY total_profit DESC
LIMIT 10;

--Анализ отмен по городам и причинам (сервисные ошибки)--
SELECT 
    city,
    store_name,
    SUM(orders_canceled_count) AS total_canceled,
    SUM(orders_canceled_service_error) AS service_errors,
    ROUND((SUM(orders_canceled_service_error)::NUMERIC / NULLIF(SUM(orders_canceled_count), 0) * 100), 2) AS service_error_share_percent
FROM mart_orders
WHERE orders_canceled_count > 0
GROUP BY city, store_name
HAVING SUM(orders_canceled_count) > 10
ORDER BY service_errors DESC;

--Топ-20 товаров по обороту (из витрины продуктов)--
SELECT 
    category_name,
    item_title,
    SUM(item_turnover)::NUMERIC AS total_turnover,
    SUM(ordered_quantity) AS total_qty_sold,
    SUM(orders_with_item_count) AS orders_count,
    ROUND(AVG(item_turnover / NULLIF(ordered_quantity, 0))::NUMERIC, 2) AS avg_price_per_item
FROM mart_products
GROUP BY category_name, item_title
ORDER BY total_turnover DESC
LIMIT 20;

--Сезонность: выручка по месяцам и годам--
SELECT 
    year,
    month,
    COUNT(DISTINCT report_date) AS days_in_report,
    SUM(orders_delivered_count) AS total_orders,
    ROUND(SUM(revenue)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(revenue)::NUMERIC, 2) AS avg_daily_revenue
FROM mart_orders
GROUP BY year, month
ORDER BY year DESC, month DESC;

--Самые популярные и непопулярные товары (используем флаги из Spark)--
SELECT 
    report_date,
    city,
    store_id,
    item_title,
    ordered_quantity,
    item_turnover
FROM mart_products
WHERE is_most_popular_day = true
ORDER BY report_date DESC, item_turnover DESC
LIMIT 250;

--Эффективность работы курьеров (смена курьера в заказе)--
SELECT 
    report_date,
    city,
    store_name,
    SUM(driver_change_count) AS total_driver_changes,
    SUM(orders_delivered_count) AS total_orders,
    ROUND((SUM(driver_change_count)::NUMERIC / NULLIF(SUM(orders_delivered_count), 0) * 100), 2) AS driver_change_rate_percent
FROM mart_orders
WHERE driver_change_count > 0
GROUP BY report_date, city, store_name
ORDER BY driver_change_rate_percent DESC
LIMIT 20;