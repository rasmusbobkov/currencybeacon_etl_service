-- Example queries to demonstrate use of the warehouse schema

-- 1. Check Total Records in Each Table
SELECT 'dim_currency' AS table_name, COUNT(*) AS total_rows FROM dim_currency
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_exchange_rate', COUNT(*) FROM fact_exchange_rate;

-- 2. Get the Latest Exchange Rate for Each Currency
SELECT 
    d.date,
    c.short_code AS currency_code,
    f.rate,
    f.base_currency
FROM fact_exchange_rate f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_currency c ON f.currency_id = c.currency_id
WHERE d.date = (SELECT MAX(date) FROM dim_date)
ORDER BY f.rate DESC;

-- 3. Check Historical Trend of a Specific Currency (Replace 'EUR' with the desired currency short code.)
SELECT 
    d.date,
    f.rate
FROM fact_exchange_rate f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_currency c ON f.currency_id = c.currency_id
WHERE c.short_code = 'EUR'
ORDER BY d.date;


--4. Compare Exchange Rates Between Two Currencies

SELECT 
    d.date,
    c1.short_code AS currency_1,
    f1.rate AS rate_1,
    c2.short_code AS currency_2,
    f2.rate AS rate_2,
    (f1.rate / f2.rate) AS exchange_rate_ratio
FROM fact_exchange_rate f1
JOIN fact_exchange_rate f2 ON f1.date_id = f2.date_id
JOIN dim_date d ON f1.date_id = d.date_id
JOIN dim_currency c1 ON f1.currency_id = c1.currency_id
JOIN dim_currency c2 ON f2.currency_id = c2.currency_id
WHERE c1.short_code = 'EUR' AND c2.short_code = 'GBP'
ORDER BY d.date;


--5. Top 5 Volatile Currencies (Highest Rate Variance Over Time)
SELECT 
    c.short_code AS currency_code,
    MAX(f.rate) - MIN(f.rate) AS rate_variance
FROM fact_exchange_rate f
JOIN dim_currency c ON f.currency_id = c.currency_id
GROUP BY c.short_code
ORDER BY rate_variance DESC
LIMIT 5;