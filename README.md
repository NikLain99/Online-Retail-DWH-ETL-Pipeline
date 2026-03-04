# Online-Retail-DWH-ETL-Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Yes-lightblue)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen)

## 🚀 Overview

**Online-Retail-DWH-ETL-Pipeline** is a full **ETL pipeline with a three-layer Data Warehouse** built for online retail analytics.

> ⚠️ Note: This is my **first project as a Data Engineer**, so please don’t judge too strictly 😉.

This project demonstrates:

* Ingesting data from Excel into **PostgreSQL**
* Logging and auditing of every batch
* Data cleansing and transformation
* Building a **Star Schema**: dimensions (`dim_customer`, `dim_product`, `dim_date`) and fact (`fact_sales`)
* Stable error handling with transactions and rollback

This is a **first professional DWH project** created using **Docker, Python, and Postgres**, aimed at mastering Data Engineering skills.

---

## 🏗 Architecture

```text
Raw Layer           → staging.stg_orders_raw_clean
                      └── staging.stg_orders_valid
Staging Layer        → clean, deduplicated, validated
Business Layer       → dim_customer, dim_product, dim_date, fact_sales
```

* **RAW Layer**: direct copy from Excel → `raw_orders` table
* **STAGING Layer**: deduplication, NULL checks, type casting, data validation
* **BUSINESS Layer**: dimension and fact tables for Star Schema

---

## ⚡ Features

* **Complete ETL process**: from Excel to analytical model
* **Audit and logging**: batch start/finish, row count, errors
* **Idempotency**: rerunnable pipeline without duplicates
* **Error handling**: automatic rollback on failure, detailed error messages
* **Star Schema**: ready for BI and SQL queries

---

## 🛠 Tech Stack

| Layer / Tool  | Purpose                                     |
| ------------- | ------------------------------------------- |
| Python 3.11   | ETL and data transformation                 |
| Pandas        | Excel → DataFrame, cleaning, transformation |
| PostgreSQL 15 | Raw, Staging, Business layers               |
| Docker        | Local environment for DB and pipeline       |
| openpyxl      | Excel reading                               |
| dotenv        | Secure configuration and secrets            |
| StringIO      | Fast CSV ingestion into PostgreSQL          |

---

## ⚙️ Installation

1. Clone the repository:

```bash
git clone https://github.com/NikLain99/Online-Retail-DWH-ETL-Pipeline.git
cd Online-Retail-DWH-ETL-Pipeline
```

2. Create `.env` and `.env.secret` files with Postgres settings:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=online_retail
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
```

3. Start Postgres via Docker (optional):

```bash
docker compose up -d
```

4. Install dependencies:

```bash
pip install -r requirements.txt
```

---

## 🏃 Usage

Run the pipeline:

```bash
python main.py datasets/Online Retail.xlsx
```

* Logging is output to console and saved in **audit tables** (`raw_load_log`, `staging_load_log`, `business_load_log`).
* On error, the pipeline automatically performs rollback and logs the error message.

---

## 📊 Data Model

**Star Schema:**

* **Dimensions**

  * `dim_customer`: customer_id, country, first_invoice, last_invoice
  * `dim_product`: stock_code, description
  * `dim_date`: invoice_date, day, month, year, weekday

* **Fact**

  * `fact_sales`: invoice_no, stock_code, customer_id, invoice_date_id, quantity, unit_price, return_flag

---

## 🔥 Highlights / Achievements

* First project with **full DWH and Star Schema**
* Handling Excel with **500k+ rows**
* Implemented **audit logging, error handling, idempotency**
* Created **Data Flow and ERD diagrams** with Crow’s Foot notation


## 🔗 Links

* [GitHub Repository](https://github.com/NikLain99/Online-Retail-DWH-ETL-Pipeline)
