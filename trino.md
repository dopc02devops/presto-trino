# Project Documentation

## Docker Commands

### 1. Start the Services
To start the services with Docker Compose, run the following command:

```bash
docker-compose up --build -d

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

SELECT AVG(CAST(SUBSTRING(blood_pressure, 1, POSITION('/' IN blood_pressure) - 1) AS INT)) AS avg_systolic
FROM "s3"."healthcare-bucket"."healthcare_data.parquet"
WHERE timestamp >= EXTRACT(EPOCH FROM DATE_SUB(CURRENT_DATE, INTERVAL 1 WEEK));

psql -h localhost -U admin -d warehouse
\dt
\d finance_data
\d+ finance_data

SELECT * FROM finance_data LIMIT 10;

SELECT DISTINCT symbol FROM finance_data;

SELECT DISTINCT cholesterol_level FROM healthcare_data;

SELECT COUNT(*) FROM finance_data;
SELECT COUNT(*) FROM healthcare_data;

SELECT COUNT(*) FROM finance_data WHERE symbol IS NULL;
SELECT COUNT(*) FROM healthcare_data WHERE cholesterol_level IS NULL;
SELECT COUNT(*) FROM finance_data WHERE close_price IS NULL;

SELECT symbol, COUNT(*) 
FROM finance_data 
GROUP BY symbol 
HAVING COUNT(*) > 1;

SELECT symbol, close_price
FROM finance_data
WHERE close_price > (SELECT AVG(close_price) + 3 * STDDEV(close_price) FROM finance_data);

CREATE VIEW finance_healthcare_summary AS
SELECT f.symbol,
       f.avg_close_price,
       f.total_market_cap,
       h.cholesterol_level,
       AVG(h.avg_age) AS avg_age,
       AVG(h.avg_bmi) AS avg_bmi
FROM finance_data f
JOIN healthcare_data h ON f.symbol = h.symbol
GROUP BY f.symbol, h.cholesterol_level;

SELECT symbol, 
       date,
       AVG(close_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg
FROM finance_data;

SELECT cholesterol_level, 
       COUNT(patient_id) 
FROM healthcare_data
GROUP BY cholesterol_level;

SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'public';
SELECT * FROM pg_stat_activity WHERE datname = 'warehouse';



