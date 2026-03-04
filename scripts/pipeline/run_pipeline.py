import psycopg2 as pg2
import pandas as pd
import os
import uuid
from dotenv import load_dotenv
from io import StringIO
import datetime

def log_start(cur, layer, batch_id, started_at, file_name=None, table=None):
    if layer == 'raw':
        cur.execute("""
            INSERT INTO audit.raw_load_log (batch_id, file_name, status, started_at)
            VALUES (%s, %s, %s, %s)
        """, (batch_id, file_name, 'STARTED', started_at))
    elif layer == 'staging':
        cur.execute("""
            INSERT INTO audit.staging_load_log (batch_id, "table", status, started_at)
            VALUES (%s, %s, %s, %s)
        """, (batch_id, table, 'STARTED', started_at))
    elif layer == 'business':
        cur.execute("""
            INSERT INTO audit.business_load_log (batch_id, "table", status, started_at)
            VALUES (%s, %s, %s, %s)
        """, (batch_id, table, 'STARTED', started_at))


def log_finish(cur, layer, batch_id, status, row_count, finished_at, error_message=None, table=None):
    if layer == 'raw':
        cur.execute("""
            UPDATE audit.raw_load_log
            SET status=%s, row_count=%s, finished_at=%s, error_message=%s
            WHERE batch_id=%s
        """, (status, row_count, finished_at, error_message, batch_id))
    elif layer == 'staging':
        cur.execute("""
            UPDATE audit.staging_load_log
            SET status=%s, row_count=%s, finished_at=%s, error_message=%s
            WHERE batch_id=%s and "table"=%s
        """, (status, row_count, finished_at, error_message, batch_id, table))
    elif layer == 'business':
        cur.execute("""
            UPDATE audit.business_load_log
            SET status=%s, row_count=%s, finished_at=%s, error_message=%s
            WHERE batch_id=%s and "table"=%s
        """, (status, row_count, finished_at, error_message, batch_id, table))


def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Excel DataFrame to match DB schema and types"""

    df = df.rename(columns={
        'InvoiceNo': 'invoice_no',
        'StockCode': 'stock_code',
        'Description': 'description',
        'Quantity': 'quantity',
        'InvoiceDate': 'invoice_date',
        'UnitPrice': 'unit_price',
        'CustomerID': 'customer_id',
        'Country': 'country'
    })
    return df.where(pd.notnull(df), None)

def now():
    return datetime.datetime.now()

def run_pipeline(file_path: str):

    load_dotenv(".env")
    load_dotenv(".env.secret")

    conn = pg2.connect(
        host="localhost",
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

    cur = conn.cursor()
    batch_id = str(uuid.uuid4())

    #=========RAW-LAYER===========
    layer = 'raw'
    started_at = now()
    log_start(cur=cur, layer=layer, batch_id=batch_id, started_at=started_at, file_name=file_path)
    conn.commit()

    try:
        print(f"{now()}:[INFO] Batch {batch_id} - RAW layer started {started_at}")
        #=============INTGEST-RAW==============#
        cur.execute("TRUNCATE TABLE raw.raw_orders CASCADE;")

        # Read entire Excel file
        df = pd.read_excel(file_path, dtype=str, engine="openpyxl")
        df = transform_df(df)
        row_count = len(df)

        # Convert DataFrame to in-memory CSV
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep=',', na_rep='\\N')
        buffer.seek(0)

        # COPY into Postgres
        cur.copy_expert("""
            COPY raw.raw_orders (
                invoice_no, stock_code, description,
                quantity, invoice_date,
                unit_price, customer_id, country
            )
            FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER ',',
                NULL '\\N'
            )
        """, buffer)

        #=============LOG-RAW-SUCCESS==============#
        finished_at = now()
        log_finish(cur=cur, 
                   layer=layer, 
                   batch_id=batch_id, 
                   status='SUCCESS', 
                   row_count=row_count, 
                   finished_at=finished_at)
        conn.commit()

        print(f"{now()}:[INFO] Batch {batch_id} - RAW layer SUCCESS ({row_count} rows)")
        print(f"{now()}:[INFO] Duration: {finished_at - started_at}")
        print("================================================")

    except Exception as e:
        conn.rollback()
        finished_at = now()
        log_finish(cur=cur, 
                   layer=layer,
                   batch_id=batch_id, 
                   status='FAILED', 
                   row_count=None, 
                   finished_at=finished_at, 
                   error_message=str(e))
        conn.commit()

        print(f"{now()}:[ERROR] Batch {batch_id} - RAW layer FAILED:", e)
        print("================================================")
        raise
    #=========STAGING-LAYER===========
    print(f"{now()}:[INFO] Batch {batch_id} - STAGING layer started")
    layer = 'staging'
    #RAW → stg_orders_raw_clean
    table='stg_orders_raw_clean'
    started_at = now()
    log_start(cur=cur,
              table=table,
              layer=layer,
              batch_id=batch_id,
              started_at=started_at,
              file_name=file_path)
    conn.commit()

    try:
        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} started")
        
        cur.execute("TRUNCATE TABLE staging.stg_orders_raw_clean CASCADE;") #truncate table

        cur.execute("""
            INSERT INTO staging.stg_orders_raw_clean(
                invoice_no,
                stock_code,
                description,
                quantity,
                invoice_date,
                unit_price,
                customer_id,
                country
            )
            SELECT
                UPPER(TRIM(invoice_no)),
                UPPER(TRIM(stock_code)),
                UPPER(TRIM(description)),
                quantity::INT,
                invoice_date::TIMESTAMP,
                unit_price::NUMERIC(12,2),
                customer_id::INT,
                UPPER(TRIM(country))
            FROM raw.raw_orders
        """)

        #=============LOG-stg_orders_raw_clean-SUCCESS==============#
        cur.execute("SELECT COUNT(*) FROM staging.stg_orders_raw_clean;")
        row_count = cur.fetchone()[0]

        finished_at = now()
        log_finish(cur=cur, 
                   layer=layer, 
                   batch_id=batch_id,
                   table=table,
                   status='SUCCESS', 
                   row_count=row_count, 
                   finished_at=finished_at)
        conn.commit()

        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} SUCCESS ({row_count} rows)")
        print(f"{now()}:[INFO] Duration: {finished_at - started_at}")
        print("================================================")

    except Exception as e:
        conn.rollback()
        finished_at = now()
        log_finish(cur=cur,
                   layer=layer,
                   table=table,
                   batch_id=batch_id, 
                   status='FAILED',
                   row_count=None, 
                   finished_at=finished_at, 
                   error_message=str(e))
        conn.commit()
        print(f"{now()}:[ERROR] Batch {batch_id} - Ingest to {table} FAILED:", e)
        print("================================================")
        raise

    #stg_orders_raw_clean → stg_orders_valid
    table='stg_orders_valid'
    started_at = now()
    log_start(cur=cur,
              table=table,
              layer=layer,
              batch_id=batch_id,
              started_at=started_at,
              file_name=file_path)
    conn.commit()

    try:
        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} started")
        cur.execute("""TRUNCATE TABLE staging.stg_orders_valid""")

        cur.execute("""
            INSERT INTO staging.stg_orders_valid(
                invoice_no,
                stock_code,
                description,
                quantity,
                invoice_date,
                unit_price,
                customer_id,    
                country
            )
            SELECT
                invoice_no,
                stock_code,
                description,
                quantity,
                invoice_date,
                unit_price,
                customer_id,    
                country
            FROM (
                SELECT 
                        *,
                    ROW_NUMBER() OVER (
                        PARTITION BY invoice_no, stock_code, invoice_date, quantity, customer_id
                        ORDER BY invoice_no
                    ) as rn
                FROM staging.stg_orders_raw_clean
                WHERE
                    customer_id IS NOT NULL
                    AND invoice_no IS NOT NULL
                    AND stock_code IS NOT NULL
            ) t
            WHERE rn=1;
        """)

        #=============LOG-stg_orders_raw_clean-SUCCESS==============#
        cur.execute("SELECT COUNT(*) FROM staging.stg_orders_valid;")
        row_count = cur.fetchone()[0]
        
        finished_at = now()
        log_finish(cur=cur, 
                   layer=layer, 
                   batch_id=batch_id,
                   table=table,
                   status='SUCCESS', 
                   row_count=row_count, 
                   finished_at=finished_at)
        conn.commit()

        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} SUCCESS ({row_count} rows)")
        print(f"{now()}:[INFO] Duration: {finished_at - started_at}")
        print("================================================")

    except Exception as e:
        conn.rollback()
        finished_at = now()
        log_finish(cur=cur,
                   layer=layer,
                   table=table,
                   batch_id=batch_id, 
                   status='FAILED',
                   row_count=None, 
                   finished_at=finished_at, 
                   error_message=str(e))
        conn.commit()
        print(f"{now()}:[ERROR] Batch {batch_id} - Ingest to {table} FAILED:", e)
        print("================================================")
        raise
    #=========Business-LAYER===========
    layer='business'
    #--------dim_customer---------
    table='dim_customer'
    started_at = now()
    log_start(cur=cur,
              table=table,
              layer=layer,
              batch_id=batch_id,
              started_at=started_at,
              file_name=file_path)
    conn.commit()

    try:
        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} started")
        cur.execute("""TRUNCATE TABLE business.dim_customer""")

        #the most popular country for the client is selected
        cur.execute("""
            WITH country_counts AS (
                SELECT
                    customer_id,
                    country,
                    COUNT(*) AS orders_count
                FROM staging.stg_orders_valid
                GROUP BY customer_id, country
            ),
            max_country AS (
                SELECT DISTINCT ON (customer_id)
                    customer_id,
                    country
                FROM
                    country_counts
                ORDER BY customer_id, orders_count desc
            ),
            invoices AS (
                SELECT
                    customer_id,
                    MIN(invoice_date) AS first_invoice,
                    MAX(invoice_date) AS last_invoice
                FROM    
                    staging.stg_orders_valid
                GROUP BY
                    customer_id
            )
            INSERT INTO business.dim_customer(
                customer_id,
                country,
                first_invoice,
                last_invoice
            )
            SELECT
                i.customer_id,
                m.country,
                i.first_invoice,
                i.last_invoice
            FROM
                invoices i
                JOIN max_country m
                    on i.customer_id=m.customer_id
        """)

        #=============LOG-stg_orders_raw_clean-SUCCESS==============#
        cur.execute("SELECT COUNT(*) FROM business.dim_customer;")
        row_count = cur.fetchone()[0]
        
        finished_at = now()
        log_finish(cur=cur, 
                   layer=layer, 
                   batch_id=batch_id,
                   table=table,
                   status='SUCCESS', 
                   row_count=row_count, 
                   finished_at=finished_at)
        conn.commit()

        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} SUCCESS ({row_count} rows)")
        print(f"{now()}:[INFO] Duration: {finished_at - started_at}")
        print("================================================")

    except Exception as e:
        conn.rollback()
        finished_at = now()
        log_finish(cur=cur,
                   layer=layer,
                   table=table,
                   batch_id=batch_id, 
                   status='FAILED',
                   row_count=None, 
                   finished_at=finished_at, 
                   error_message=str(e))
        conn.commit()
        print(f"{now()}:[ERROR] Batch {batch_id} - Ingest to {table} FAILED:", e)
        print("================================================")
        raise
    #--------dim_product---------
    table='dim_product'
    started_at = now()
    log_start(cur=cur,
              table=table,
              layer=layer,
              batch_id=batch_id,
              started_at=started_at,
              file_name=file_path)
    conn.commit()
    try:
        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} started")
        cur.execute("""TRUNCATE TABLE business.dim_product""")

        cur.execute("""
            INSERT INTO business.dim_product(stock_code, description)
            SELECT stock_code, description
            FROM (
                SELECT stock_code,
                    description,
                    ROW_NUMBER() OVER (
                        PARTITION BY stock_code 
                        ORDER BY cnt DESC, description
                    ) AS rn
                FROM (
                    SELECT stock_code, description, COUNT(*) AS cnt
                    FROM staging.stg_orders_valid
                    GROUP BY stock_code, description
                ) t1
            ) t2
            WHERE rn = 1;
        """)

        #=============LOG-stg_orders_raw_clean-SUCCESS==============#
        cur.execute("SELECT COUNT(*) FROM business.dim_product;")
        row_count = cur.fetchone()[0]
        
        finished_at = now()
        log_finish(cur=cur, 
                   layer=layer, 
                   batch_id=batch_id,
                   table=table,
                   status='SUCCESS', 
                   row_count=row_count, 
                   finished_at=finished_at)
        conn.commit()

        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} SUCCESS ({row_count} rows)")
        print(f"{now()}:[INFO] Duration: {finished_at - started_at}")
        print("================================================")
        
    except Exception as e:
        conn.rollback()
        finished_at = now()
        log_finish(cur=cur,
                   layer=layer,
                   table=table,
                   batch_id=batch_id, 
                   status='FAILED',
                   row_count=None, 
                   finished_at=finished_at, 
                   error_message=str(e))
        conn.commit()
        print(f"{now()}:[ERROR] Batch {batch_id} - Ingest to {table} FAILED:", e)
        print("================================================")
        raise
    #--------dim_date---------
    table='dim_date'
    started_at = now()
    log_start(cur=cur,
              table=table,
              layer=layer,
              batch_id=batch_id,
              started_at=started_at,
              file_name=file_path)
    conn.commit()
    try:
        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} started")
        cur.execute("""TRUNCATE TABLE business.dim_date""")

        cur.execute("""
            INSERT INTO business.dim_date(invoice_date, day, month, year, weekday)
            SELECT DISTINCT
                invoice_date,
                EXTRACT(DAY FROM invoice_date)::INT,
                EXTRACT(MONTH FROM invoice_date)::INT,
                EXTRACT(YEAR FROM invoice_date)::INT,
                EXTRACT(DOW FROM invoice_date)::INT
            FROM staging.stg_orders_valid;
        """)

        #=============LOG-stg_orders_raw_clean-SUCCESS==============#
        cur.execute("SELECT COUNT(*) FROM business.dim_date;")
        row_count = cur.fetchone()[0]
        
        finished_at = now()
        log_finish(cur=cur, 
                   layer=layer, 
                   batch_id=batch_id,
                   table=table,
                   status='SUCCESS', 
                   row_count=row_count, 
                   finished_at=finished_at)
        conn.commit()

        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} SUCCESS ({row_count} rows)")
        print(f"{now()}:[INFO] Duration: {finished_at - started_at}")
        print("================================================")
        
    except Exception as e:
        conn.rollback()
        finished_at = now()
        log_finish(cur=cur,
                   layer=layer,
                   table=table,
                   batch_id=batch_id, 
                   status='FAILED',
                   row_count=None, 
                   finished_at=finished_at, 
                   error_message=str(e))
        conn.commit()
        print(f"{now()}:[ERROR] Batch {batch_id} - Ingest to {table} FAILED:", e)
        print("================================================")
        raise
    #--------fact_sales---------
    table='fact_sales'
    started_at = now()
    log_start(cur=cur,
              table=table,
              layer=layer,
              batch_id=batch_id,
              started_at=started_at,
              file_name=file_path)
    conn.commit()
    try:
        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} started")
        cur.execute("""TRUNCATE TABLE business.fact_sales""")

        cur.execute("""
            INSERT INTO business.fact_sales(
                invoice_no,
                stock_code,
                customer_id,
                invoice_date_id,
                quantity,
                unit_price,
                return_flag
            )
            SELECT s.invoice_no,
                p.stock_code,
                c.customer_id,
                d.invoice_date_id,
                s.quantity,
                s.unit_price,
                CASE 
                    WHEN invoice_no LIKE 'C%' THEN  true
                    ELSE FALSE
                END
            FROM staging.stg_orders_valid s
            JOIN business.dim_date d
            ON s.invoice_date = d.invoice_date
            JOIN business.dim_product p
            ON s.stock_code = p.stock_code
            JOIN business.dim_customer c
            ON s.customer_id = c.customer_id;
        """)

        #=============LOG-stg_orders_raw_clean-SUCCESS==============#
        cur.execute("SELECT COUNT(*) FROM business.fact_sales;")
        row_count = cur.fetchone()[0]
        
        finished_at = now()
        log_finish(cur=cur, 
                   layer=layer, 
                   batch_id=batch_id,
                   table=table,
                   status='SUCCESS', 
                   row_count=row_count, 
                   finished_at=finished_at)
        conn.commit()

        print(f"{now()}:[INFO] Batch {batch_id} - Ingest to {table} SUCCESS ({row_count} rows)")
        print(f"{now()}:[INFO] Duration: {finished_at - started_at}")
        print("================================================")
        
    except Exception as e:
        conn.rollback()
        finished_at = now()
        log_finish(cur=cur,
                   layer=layer,
                   table=table,
                   batch_id=batch_id, 
                   status='FAILED',
                   row_count=None, 
                   finished_at=finished_at, 
                   error_message=str(e))
        conn.commit()
        print(f"{now()}:[ERROR] Batch {batch_id} - Ingest to {table} FAILED:", e)
        print("================================================")
        raise
    
    finally:
        cur.close()
        conn.close()
    
if __name__ == "__main__":
    run_pipeline("datasets/Online Retail.xlsx")