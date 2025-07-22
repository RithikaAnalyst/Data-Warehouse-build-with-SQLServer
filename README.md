# SQL Data Warehouse Project

This project demonstrates the implementation of a data warehouse using **SQL Server**. It includes the full pipeline from staging raw data to building the **Gold Layer** for reporting and analytics.

---

## Project Overview

The data warehouse is structured into three layers:
- **Staging Layer**: Stores raw data extracted from source systems.
- **Silver Layer**: Contains cleaned and transformed data.
- **Gold Layer**: Optimized business-level tables (facts and dimensions) for reporting and analysis.

This architecture supports reporting, dashboarding, and business intelligence use cases.

![data architecture](<img width="1342" height="678" alt="data architucture picture" src="https://github.com/user-attachments/assets/28f42d5a-ae4f-4fed-9e0a-0284827e7841" />
cture picture.png)

---

## Schema & Tables

### Staging Layer (`stg`)
- Raw imported data with minimal transformations.

### Silver Layer (`silver`)
- Cleaned and normalized data.
- Keys, deduplication, and base-level transformations.

### Gold Layer (`gold`)
- Fact and dimension tables.
- Examples:
  - `gold.dim_customers`_
### ERD Diagram (Gold Layer)
                 +---------------------+
                 |  dim_customers      |
                 +---------------------+
                 | customer_key (PK)   |
                 | customer_id         |
                 | customer_number     |
                 | first_name          |
                 | last_name           |
                 | gender              |
                 | birthdate           |
                 | country             |
                 | marital_status      |
                 | create_date         |
                 +---------------------+

                 +---------------------+
                 |  dim_products       |
                 +---------------------+
                 | product_key (PK)    |
                 | product_id          |
                 | product_number      |
                 | product_name        |
                 | category_id         |
                 | category            |
                 | subcategory         |
                 | product_line        |
                 | cost                |
                 | start_date          |
                 +---------------------+

                 +---------------------+
                 |     fact_sales      |
                 +---------------------+
                 | order_number        |
                 | customer_key (FK)   |
                 | product_key (FK)    |
                 | order_date          |
                 | shipping_date       |
                 | due_date            |
                 | quantity            |
                 | price               |
                 | sales_amount        |
                 +---------------------+
 
## Building the Data Warehouse (Data Engineering)
# Objective
  Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

# Specifications
- Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
- Data Quality: Cleanse and resolve data quality issues prior to analysis.
- Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
- Scope: Focus on the latest dataset only; historization of data is not required.
- Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

## BI: Analytics & Reporting (Data Analysis)
# Objective
Develop SQL-based analytics to deliver detailed insights into:

- Customer Behavior
- Product Performance
- Sales Trends
These insights empower stakeholders with key business metrics, enabling strategic decision-making.
