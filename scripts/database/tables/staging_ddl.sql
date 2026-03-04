DROP TABLE IF EXISTS staging.stg_orders_raw_clean CASCADE;
CREATE TABLE staging.stg_orders_raw_clean(
    invoice_no varchar(20),
    stock_code varchar(20),
    description varchar(50),
    quantity integer,
    invoice_date timestamp without time zone,
    unit_price numeric(10,2),
    customer_id integer,
    country varchar(50),
    ingestion_ts timestamp without time zone DEFAULT now()
);

DROP TABLE IF EXISTS staging.stg_orders_valid CASCADE;
CREATE TABLE staging.stg_orders_valid(
    invoice_no varchar(20),
    stock_code varchar(20),
    description varchar(50),
    quantity integer,
    invoice_date timestamp without time zone,
    unit_price numeric(10,2),
    customer_id integer,
    country varchar(50),
    ingestion_ts timestamp without time zone
);