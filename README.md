# Olist E-commerce Analytics

An end to end analytics engineering project built on the modern data stack. Raw Brazilian e-commerce data ingested via Python into Snowflake, transformed through three dbt modeling layers, and visualized in a 4 page Power BI dashboard.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Dataset](#dataset)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Models](#data-models)
- [Dashboard](#dashboard)
- [Key Insights](#key-insights)
- [How to Reproduce](#how-to-reproduce)
- [Author](#author)

---

## Project Overview

This project demonstrates a complete analytics engineering workflow on the [Olist Brazilian E-commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), a real-world dataset of 100,000+ orders placed on Brazil's largest department store marketplace between 2016 and 2018.

The project answers four core business questions:

1. How is revenue trending over time and which states drive the most sales?
2. Which product categories perform best and what does demand look like across the catalog?
3. What does the customer base look like in terms of lifetime value and segmentation?
4. How is delivery performance tracking and where are the biggest delays?

---

## Architecture

```
Kaggle CSV Files (9 tables)
        |
        v
Python Ingestion Script
(snowflake-connector-python, pandas)
        |
        v
Snowflake RAW Schema
        |
        v
dbt Staging Layer
(clean, rename, cast)
        |
        v
dbt Intermediate Layer
(join, enrich, aggregate)
        |
        v
dbt Marts Layer
(business-ready facts and dimensions)
        |
        v
Power BI Service Dashboard
(4 pages, 16+ visuals)
```

---

## Dataset

**Source:** [Olist Brazilian E-commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) via Kaggle

**9 source tables loaded into Snowflake RAW schema:**

| Table | Description | Rows |
|---|---|---|
| ORDERS | Order header with status and timestamps | 99,441 |
| ORDER_ITEMS | Line items with product, seller, price | 112,650 |
| ORDER_PAYMENTS | Payment method and value per order | 103,886 |
| ORDER_REVIEWS | Customer review scores and comments | 99,224 |
| CUSTOMERS | Customer location data | 99,441 |
| PRODUCTS | Product attributes and dimensions | 32,951 |
| SELLERS | Seller location data | 3,095 |
| GEOLOCATION | Brazilian zip code coordinates | 1,000,163 |
| PRODUCT_CATEGORY_TRANSLATION | Portuguese to English category names | 71 |

---

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, pandas, snowflake-connector-python |
| Storage | Snowflake |
| Transformation | dbt Cloud |
| Version Control | GitHub |
| Visualization | Power BI Service |

---

## Project Structure

```
ecommerce_analytics/
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_order_payments.sql
│   │   ├── stg_order_reviews.sql
│   │   ├── stg_products.sql
│   │   ├── stg_sellers.sql
│   │   └── stg_product_category_translation.sql
│   ├── intermediate/
│   │   ├── int_orders_enriched.sql
│   │   └── int_products_enriched.sql
│   └── marts/
│       ├── fct_orders.sql
│       ├── fct_product_performance.sql
│       ├── dim_customers.sql
│       └── schema.yml
├── dbt_project.yml
├── load_to_snowflake.py
└── README.md
```

---

## Data Models

### Staging Layer
Clean and rename raw source tables. One model per source table. No business logic applied at this layer.

| Model | Source Table | Purpose |
|---|---|---|
| stg_orders | ORDERS | Clean order headers, cast timestamps |
| stg_customers | CUSTOMERS | Clean customer location fields |
| stg_order_items | ORDER_ITEMS | Cast price and freight to float |
| stg_order_payments | ORDER_PAYMENTS | Cast payment values and types |
| stg_order_reviews | ORDER_REVIEWS | Clean review scores and timestamps |
| stg_products | PRODUCTS | Clean product dimensions and category |
| stg_sellers | SELLERS | Clean seller location fields |
| stg_product_category_translation | PRODUCT_CATEGORY_TRANSLATION | Map Portuguese to English category names |

### Intermediate Layer
Join and enrich staging models. Aggregations and business logic introduced here.

| Model | Description |
|---|---|
| int_orders_enriched | Joins orders, customers, items, payments, and reviews into one enriched order record. Calculates actual and estimated delivery days. |
| int_products_enriched | Joins products with English category translations and aggregated order item metrics per product. |

### Marts Layer
Business ready models consumed directly by Power BI.

| Model | Type | Description |
|---|---|---|
| fct_orders | Fact | One row per order with all enriched fields and a delivery status flag (On Time, Late, Not Delivered) |
| fct_product_performance | Fact | One row per product with revenue, order count, average price, and demand tier classification |
| dim_customers | Dimension | One row per unique customer with lifetime value, order history, and customer segment (High, Mid, Low Value) |

### dbt Tests
The following tests are applied to mart models via schema.yml:

- fct_orders: order_id is unique and not null, customer_id is not null, order_status is not null
- fct_product_performance: product_id is unique and not null
- dim_customers: customer_unique_id is unique and not null

---

## Dashboard

Built in Power BI Service with 4 report pages connected directly to the Snowflake STAGING_MARTS schema.

**Page 1: Revenue Overview**
- Total revenue (16.01M BRL), total orders (99.4K), average order value (160.99 BRL)
- Revenue trend line chart by month
- Revenue by customer state bar chart

**Page 2: Product Performance**
- Total product revenue card (13.59M BRL)
- Top 10 product categories by revenue
- Demand tier breakdown donut chart (83.6% Low, 9.45% High, 6.95% Medium)
- Average price by product category

**Page 3: Customer Analysis**
- Total unique customers (96K), average lifetime value (172.64 BRL)
- Customer segment donut chart (94.92% Low Value, 3.72% High Value)
- Top 10 states by customer count with SP dominating

**Page 4: Delivery Performance**
- Average actual delivery days (12.50) vs estimated (24.40)
- Delivery status donut (90.45% On Time, 6.57% Late, 2.98% Not Delivered)
- Average delivery days by state bar chart

---

## Key Insights

1. **Olist significantly over-promises on delivery.** The average estimated delivery is 24.4 days but actual delivery averages 12.5 days, nearly half. This is a strong driver of positive customer experience.

2. **São Paulo dominates the customer base.** SP accounts for the largest share of customers by a wide margin, followed by RJ and MG. Marketing and logistics investments should be weighted accordingly.

3. **Health and beauty leads all product categories** by revenue, followed by watches and gifts, and bed, bath and table. These three categories represent the top revenue concentration.

4. **83.6% of products sit in the Low Demand tier,** suggesting a long tail catalog where a small percentage of SKUs drive the majority of volume. Classic 80/20 distribution.

5. **94.92% of customers are in the Low Value segment** with a lifetime value below 500 BRL. There is significant upside opportunity in developing a retention and upsell strategy for the mid and high value segments.

6. **Northern states (RR, AP, AM) have the longest delivery times,** averaging close to 30 days, versus southern states which average under 10 days. A strong logistics gap exists across regions.

---

## How to Reproduce

### Prerequisites
- Snowflake account (free trial at snowflake.com)
- dbt Cloud account (free developer account at cloud.getdbt.com)
- Python 3.8 or higher
- Kaggle account to download the dataset

### Step 1: Download the dataset
Download the Olist Brazilian E-commerce Dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and unzip the CSV files.

### Step 2: Set up Snowflake
Run the following SQL in a Snowflake worksheet:

```sql
CREATE WAREHOUSE IF NOT EXISTS DEV_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DB.RAW;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DB.MARTS;

CREATE ROLE IF NOT EXISTS DBT_ROLE;
CREATE USER IF NOT EXISTS DBT_USER
  PASSWORD = 'YourPassword'
  DEFAULT_ROLE = DBT_ROLE
  DEFAULT_WAREHOUSE = DEV_WH;

GRANT ROLE DBT_ROLE TO USER DBT_USER;
GRANT USAGE ON WAREHOUSE DEV_WH TO ROLE DBT_ROLE;
GRANT ALL ON DATABASE ECOMMERCE_DB TO ROLE DBT_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE ECOMMERCE_DB TO ROLE DBT_ROLE;
```

### Step 3: Install Python dependencies

```bash
pip install pandas "snowflake-connector-python[pandas]"
```

### Step 4: Run the ingestion script
Update the connection credentials and CSV path in `load_to_snowflake.py` then run:

```bash
python load_to_snowflake.py
```

This loads all 9 CSV files into the ECOMMERCE_DB.RAW schema.

### Step 5: Set up dbt Cloud
1. Create a free account at [cloud.getdbt.com](https://cloud.getdbt.com)
2. Create a new project and connect to Snowflake using your DBT_USER credentials
3. Connect your GitHub repository
4. Set your development credentials: schema = STAGING, target = dev, threads = 4

### Step 6: Run dbt models

```bash
dbt run
dbt test
```

### Step 7: Connect Power BI
1. Open Power BI Service at [app.powerbi.com](https://app.powerbi.com)
2. Create a new report and connect to Snowflake
3. Select ECOMMERCE_DB and the STAGING_MARTS schema
4. Load fct_orders, fct_product_performance, and dim_customers
5. Build your dashboards using the four page structure above

---

## Author

**Ivy Chisom Adiele Khalid**
Data Analyst and Analytics Engineer, Lagos Nigeria
Founder of DatumStack

- Portfolio: [adannee.github.io/IvyAdiele.github.io](https://adannee.github.io/IvyAdiele.github.io)
- LinkedIn: [linkedin.com/in/ivy-khalid](https://linkedin.com/in/ivy-khalid)
- GitHub: [github.com/Adannee](https://github.com/Adannee)
- DatumStack: [@datumstack_](https://www.linkedin.com/company/datumstack_)
