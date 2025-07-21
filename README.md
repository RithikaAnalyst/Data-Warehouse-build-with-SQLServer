# SQL Data Warehouse Project

This project demonstrates the implementation of a data warehouse using **SQL Server**. It includes the full pipeline from staging raw data to building the **Gold Layer** for reporting and analytics.

---

## Project Overview

The data warehouse is structured into three layers:
- **Staging Layer**: Stores raw data extracted from source systems.
- **Silver Layer**: Contains cleaned and transformed data.
- **Gold Layer**: Optimized business-level tables (facts and dimensions) for reporting and analysis.

This architecture supports reporting, dashboarding, and business intelligence use cases.

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
