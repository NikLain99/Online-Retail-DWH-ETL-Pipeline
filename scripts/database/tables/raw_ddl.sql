DROP TABLE IF EXISTS raw.raw_orders CASCADE;
CREATE TABLE raw.raw_orders (
    invoice_no TEXT,
    stock_code TEXT,
    description TEXT,
    quantity TEXT,
    invoice_date TEXT,
    unit_price TEXT,
    customer_id TEXT,
    country TEXT,
    ingestion_ts TIMESTAMP DEFAULT NOW()
);


