DROP TABLE IF EXISTS business.dim_customer CASCADE;
CREATE TABLE business.dim_customer(
    customer_id integer PRIMARY KEY,
    country varchar(50),
    first_invoice TIMESTAMP,
    last_invoice TIMESTAMP,
    ingestion_ts TIMESTAMP DEFAULT now()
);

DROP TABLE IF EXISTS business.dim_product CASCADE;
CREATE TABLE business.dim_product(
    stock_code varchar(20) PRIMARY KEY,
    description varchar(50),
    ingestion_ts TIMESTAMP DEFAULT now()
);

DROP TABLE IF EXISTS business.dim_date CASCADE;
CREATE TABLE business.dim_date(
    invoice_date_id SERIAL PRIMARY KEY,
    invoice_date TIMESTAMP,
    day INT,
    month INT,
    year INT,
    weekday INT,
    ingestion_ts TIMESTAMP DEFAULT now()
);

DROP Table if EXISTS business.fact_sales;
CREATE TABLE business.fact_sales(
    fact_id BIGSERIAL PRIMARY KEY,                                                  --Surrogate key
    invoice_no VARCHAR(20) NOT NULL,
    stock_code VARCHAR(20) NOT NULL REFERENCES business.dim_product(stock_code),    -- FK → dim_product
    customer_id INT NOT NULL REFERENCES business.dim_customer(customer_id),         -- FK → dim_customer
    invoice_date_id INT NOT NULL REFERENCES business.dim_date(invoice_date_id),             -- FK → dim_date
    quantity INT NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,  
    total_amount NUMERIC(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    return_flag BOOL DEFAULT FALSE NOT NULL,
    ingestion_ts TIMESTAMP NOT NULL DEFAULT now()
);