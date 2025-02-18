
docker-compose up --build -d

To check if Trino is running, open:
🔗 http://localhost:8080 (Trino UI)
🔗 http://localhost:9001 (MinIO Console - login with admin / password)


docker exec -it trino-coordinator trino
SELECT * FROM system.runtime.nodes;


SELECT symbol, AVG(close_price) AS avg_close_price
FROM "s3"."finance-bucket"."finance_data.csv"
GROUP BY symbol;

SELECT symbol, MAX(market_cap) AS max_market_cap
FROM "s3"."finance-bucket"."finance_data.csv"
GROUP BY symbol
ORDER BY max_market_cap DESC
LIMIT 1;


SELECT symbol, MAX(dividend_yield) AS highest_dividend_yield
FROM "s3"."finance-bucket"."finance_data.csv"
GROUP BY symbol
ORDER BY highest_dividend_yield DESC
LIMIT 1;

SELECT symbol, MAX(dividend_yield) AS highest_dividend_yield
FROM "s3"."finance-bucket"."finance_data.csv"
GROUP BY symbol
ORDER BY highest_dividend_yield DESC
LIMIT 1;

SELECT cholesterol_level, AVG(age) AS avg_age
FROM "s3"."healthcare-bucket"."healthcare_data.parquet"
GROUP BY cholesterol_level;

SELECT COUNT(*) AS high_blood_pressure_patients
FROM "s3"."healthcare-bucket"."healthcare_data.parquet"
WHERE CAST(SUBSTRING(blood_pressure, 1, POSITION('/' IN blood_pressure) - 1) AS INT) > 120;

SELECT AVG(bmi) AS avg_bmi
FROM "s3"."healthcare-bucket"."healthcare_data.parquet"
WHERE medication != 'None';

SELECT class_label, COUNT(*) AS count
FROM "s3"."ml-bucket"."ml_data.json"
GROUP BY class_label;

SELECT AVG(blood_sugar) AS avg_blood_sugar
FROM "s3"."ml-bucket"."ml_data.json"
WHERE cholesterol_level = 'High';

SELECT COUNT(*) AS overweight_patients
FROM "s3"."ml-bucket"."ml_data.json"
WHERE bmi > 30;

SELECT f.symbol, AVG(f.close_price) AS avg_stock_price
FROM "s3"."finance-bucket"."finance_data.csv" f
JOIN "s3"."healthcare-bucket"."healthcare_data.parquet" h
    ON f.timestamp = h.timestamp
WHERE h.age > 50
GROUP BY f.symbol;

SELECT symbol, SUM(market_cap) AS total_market_cap
FROM "s3"."finance-bucket"."finance_data.csv"
WHERE timestamp >= EXTRACT(EPOCH FROM DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH))
GROUP BY symbol;

SELECT symbol, SUM(market_cap) AS total_market_cap
FROM "s3"."finance-bucket"."finance_data.csv"
WHERE timestamp >= EXTRACT(EPOCH FROM DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH))
GROUP BY symbol;

SELECT AVG(CAST(SUBSTRING(blood_pressure, 1, POSITION('/' IN blood_pressure) - 1) AS INT)) AS avg_systolic
FROM "s3"."healthcare-bucket"."healthcare_data.parquet"
WHERE timestamp >= EXTRACT(EPOCH FROM DATE_SUB(CURRENT_DATE, INTERVAL 1 WEEK));

