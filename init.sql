-- Create Finance Data Table
CREATE TABLE IF NOT EXISTS finance_data (
    symbol TEXT PRIMARY KEY,
    avg_close_price NUMERIC,
    total_market_cap NUMERIC,
    avg_pe_ratio NUMERIC,
    avg_dividend_yield NUMERIC
);

-- Create Healthcare Data Table
CREATE TABLE IF NOT EXISTS healthcare_data (
    cholesterol_level TEXT PRIMARY KEY,
    avg_age NUMERIC,
    avg_bmi NUMERIC,
    count_patients INT
);

-- Create Machine Learning Data Table
CREATE TABLE IF NOT EXISTS ml_data (
    cholesterol_level TEXT PRIMARY KEY,
    avg_blood_sugar NUMERIC,
    count_patients INT
);
