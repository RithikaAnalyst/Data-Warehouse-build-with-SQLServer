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

```SQL Server
=======Create database and schema=======
USE master;
GO
-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

=============DDL Bronze Layer ==============


IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info
(cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE);
GO

IF OBJECT_ID('bronze.crm_prd_id','U') IS NOT NULL
DROP TABLE bronze.crm_prd_id;
GO
CREATE TABLE bronze.crm_prd_id
(prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50)	,
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
DROP TABLE bronze.crm_sales_details;
GO
CREATE TABLE bronze.crm_sales_details
(sls_ord_num NVARCHAR(50),
sls_prd_key	NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt	INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT);
GO


IF OBJECT_ID('bronze.erp_CUST_AZ12','U') IS NOT NULL
DROP TABLE bronze.erp_CUST_AZ12;
GO
CREATE TABLE bronze.erp_CUST_AZ12
(CID NVARCHAR(50),
BDATE DATETIME,
GEN NVARCHAR(50));
GO

IF OBJECT_ID('bronze.erp_LOC_A101','U') IS NOT NULL
DROP TABLE bronze.erp_LOC_A101;
GO
CREATE TABLE bronze.erp_LOC_A101
(CID NVARCHAR(50),
CNTRY NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
DROP TABLE  bronze.erp_PX_CAT_G1V2;
GO
CREATE TABLE bronze.erp_PX_CAT_G1V2
(ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50)
);
GO

==========Load Bronze Layer============


CREATE OR ALTER PROCEDURE bronze.layer_bronze AS
BEGIN
  DECLARE @START_TIME DATETIME,@END_TIME DATETIME,@BATCH_SATRTTIME DATETIME,@BATCH_ENDTIME DATETIME;
  BEGIN TRY
            PRINT'====================================='
			PRINT'BEGIN OF THE BRONZE LAYER'
			PRINT'====================================='


			PRINT'-------------------------------------'
			PRINT'BEGIN OF THE CRM TABLE'
			PRINT'-------------------------------------'
			
			PRINT'-------------'
			SET @BATCH_SATRTTIME=GETDATE();
			SET @START_TIME=GETDATE();
			TRUNCATE TABLE bronze.crm_cust_info;
			BULK INSERT bronze.crm_cust_info
			FROM'C:\Users\rithi\OneDrive\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
			SET @END_TIME=GETDATE();
			PRINT'>>>DURATION OF bronze.crm_cust_info '+CAST( DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' Seconds';
			PRINT'-------------'


			PRINT'-------------'
			SET @START_TIME=GETDATE();
			TRUNCATE TABLE bronze.crm_prd_id;
			BULK INSERT bronze.crm_prd_id
			FROM'C:\Users\rithi\OneDrive\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
			SET @END_TIME=GETDATE();
			PRINT'>>>DURATION OF bronze.crm_prd_id '+CAST( DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' Seconds';
			PRINT'-------------'

			PRINT'-------------'
			SET @START_TIME=GETDATE();
			TRUNCATE TABLE bronze.crm_sales_details;
			BULK INSERT bronze.crm_sales_details
			FROM'C:\Users\rithi\OneDrive\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
			SET @END_TIME=GETDATE();
			PRINT'>>>DURATION OF bronze.crm_sales_details '+CAST( DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' Seconds';
			PRINT'-------------'

			PRINT'-------------------------------------'
			PRINT'END OF THE CRM TABLE'
			PRINT'-------------------------------------'

			PRINT'-------------------------------------'
			PRINT'BEGIN OF THE ERP TABLE'
			PRINT'-------------------------------------'

			PRINT'-------------'
			SET @START_TIME=GETDATE();
			TRUNCATE TABLE bronze.erp_CUST_AZ12;
			BULK INSERT bronze.erp_CUST_AZ12
			FROM'C:\Users\rithi\OneDrive\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
			SET @END_TIME=GETDATE();
			PRINT'>>>DURATION OF bronze.erp_CUST_AZ12 '+CAST( DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' Seconds';
			PRINT'-------------'

            PRINT'-------------'
            SET @START_TIME=GETDATE();
			TRUNCATE TABLE bronze.erp_LOC_A101;
			BULK INSERT bronze.erp_LOC_A101
			FROM'C:\Users\rithi\OneDrive\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
			SET @END_TIME=GETDATE();
			PRINT'>>>DURATION OF bronze.erp_LOC_A101 '+CAST( DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' Seconds';
			PRINT'-------------'

			PRINT'-------------'
			SET @START_TIME=GETDATE();
			TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
			BULK INSERT bronze.erp_PX_CAT_G1V2
			FROM'C:\Users\rithi\OneDrive\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
			SET @END_TIME=GETDATE();
			PRINT'>>>DURATION OF bronze.erp_PX_CAT_G1V2 '+CAST( DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR)+' Seconds';
			PRINT'-------------'


			
			PRINT'-------------------------------------'
			PRINT'END OF THE ERP FIEL'
			PRINT'-------------------------------------'
			
			PRINT'====================================='
			PRINT'END OF THE BRONZE LAYER'
			PRINT'====================================='
			SET @BATCH_ENDTIME=GETDATE();
			PRINT'>>>DURATION OF THE BRONZE LAYER '+ CAST (DATEDIFF(SECOND,@BATCH_SATRTTIME,@BATCH_ENDTIME)AS NVARCHAR)+ '  Seconds';		
  END TRY
  BEGIN CATCH
	  PRINT '>>>>Error occured during loading a bronze layer'
	  PRINT'>>>>error message'+ ERROR_MESSAGE();
	  PRINT'>>>>error number'+ CAST (ERROR_NUMBER() AS NVARCHAR);
	  PRINT'>>>>error state'+ CAST (ERROR_STATE() AS NVARCHAR);
  END CATCH
END

===========DDL Silver Layer=============

IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info
(cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.crm_prd_id','U') IS NOT NULL
DROP TABLE silver.crm_prd_id;

CREATE TABLE silver.crm_prd_id
(prd_id INT,
prd_key NVARCHAR(50),
cat_id NVARCHAR(50),
sprd_key NVARCHAR(50),
prd_nm NVARCHAR(50)	,
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details
(sls_ord_num NVARCHAR(50),
sls_prd_key	NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt	DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE());



IF OBJECT_ID('silver.erp_CUST_AZ12','U') IS NOT NULL
DROP TABLE silver.erp_CUST_AZ12;

CREATE TABLE silver.erp_CUST_AZ12
(CID NVARCHAR(50),
BDATE DATETIME,
GEN NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE());


IF OBJECT_ID('silver.erp_LOC_A101','U') IS NOT NULL
DROP TABLE silver.erp_LOC_A101;

CREATE TABLE silver.erp_LOC_A101
(CID NVARCHAR(50),
CNTRY NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.erp_PX_CAT_G1V2','U') IS NOT NULL
DROP TABLE  silver.erp_PX_CAT_G1V2;

CREATE TABLE silver.erp_PX_CAT_G1V2
(ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

============= Load Silver Layer =============

CREATE OR ALTER PROCEDURE silver.layer_silver AS
BEGIN
  BEGIN TRY
           DECLARE @START_TIME DATETIME,@END_TIME DATETIME,@BATCHSTART_TIME DATETIME,@BATCHEND_TIME DATETIME
                        
            PRINT'=============================='
            PRINT'BEGIN OF SILVER LAYER'
            PRINT'=============================='
            PRINT'-------------------------------'
            PRINT'BEGINT OF silver.crm TABLE'
             PRINT'-------------------------------'
            SET @BATCHSTART_TIME=GETDATE();
            SET  @START_TIME=GETDATE();
            TRUNCATE TABLE silver.crm_cust_info;
            INSERT INTO silver.crm_cust_info(
            cst_id ,
            cst_key ,
            cst_firstname ,
            cst_lastname ,
            cst_marital_status ,
            cst_gndr ,
            cst_create_date)

             SELECT 
                 cst_id,
                 cst_key,
                 TRIM(cst_firstname) AS cst_firstname,
                 TRIM(cst_lastname) AS cst_lastname ,
             CASE WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
                  WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single'
                  ELSE 'N/A'
            END cst_marital_status,

            CASE  WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
                  WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
                  ELSE 'N/A'
            END  cst_gndr,
                 cst_create_date
            FROM 
            (SELECT *,
            ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Rownum
            FROM bronze.crm_cust_info)AS sub
            WHERE Rownum = 1;
            SET @END_TIME=GETDATE();
            PRINT'>>>DURATION OF silver.crm_cust_info '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR) + ' SECONDS'

            PRINT'------------------------------------------------------'
            SET  @START_TIME=GETDATE();
            TRUNCATE TABLE silver.crm_prd_id;
            INSERT INTO silver.crm_prd_id
            (prd_id ,
            prd_key ,
            cat_id ,
            sprd_key ,
            prd_nm 	,
            prd_cost ,
            prd_line ,
            prd_start_dt ,
            prd_end_dt  
            )
            SELECT 
            prd_id ,
            prd_key,
            REPLACE(SUBSTRING (prd_key,1,5) ,'-','_')AS cat_id,
            SUBSTRING(prd_key,7,len(prd_key)) AS sprd_key,
            prd_nm 	,
            ISNULL(prd_cost,0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                 WHEN 'R' THEN 'Road'
                 WHEN 'M' THEN 'Mountain'
                 WHEN 'S' THEN 'Other sales'
                 WHEN 'T' THEN 'Touring'
                 ELSE 'N/A'
            END prd_line,
            CAST (prd_start_dt AS DATE) AS prd_start_dt  ,
            CAST (LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE)AS  prd_end_dt 
            FROM bronze.crm_prd_id;
            SET @END_TIME=GETDATE();
            PRINT'>>>DURATION OF silver.crm_prd_id '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR) + ' SECONDS'

            PRINT'------------------------------------------------------'
            SET  @START_TIME=GETDATE();
            TRUNCATE TABLE silver.crm_sales_details;
            
            INSERT INTO silver.crm_sales_details
            (sls_ord_num ,
            sls_prd_key	,
            sls_cust_id ,
            sls_order_dt ,
            sls_ship_dt	,
            sls_due_dt ,
            sls_sales ,
            sls_quantity ,
            sls_price )
            SELECT 
            sls_ord_num ,
            sls_prd_key	,
            sls_cust_id ,

            CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL  
                 ELSE CAST (sls_order_dt AS NVARCHAR) 
            END sls_order_dt,

            CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL  
                 ELSE CAST (sls_ship_dt AS NVARCHAR) 
            END sls_ship_dt,	

            CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL  
                 ELSE CAST (sls_due_dt AS NVARCHAR) 
            END sls_due_dt, 

            CASE WHEN  sls_sales != sls_quantity * ABS (sls_price) OR sls_sales IS NULL OR sls_sales <=0 
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales
            END sls_sales ,
 
            CASE WHEN sls_price IS NULL OR sls_price <=0 
                 THEN sls_sales/sls_quantity 
                 ELSE sls_price 
            END sls_price ,

            sls_quantity   
            FROM bronze.crm_sales_details;
            SET @END_TIME=GETDATE();
            PRINT'>>>DURATION OF silver.crm_sales_details '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR) + ' SECONDS'

             PRINT'-------------------------------'
             PRINT'END OF SILVER CRM TABLE'
             PRINT'-------------------------------'
             PRINT'-------------------------------'
             PRINT'BEGIN OF SILVER ERP TABLE'
             PRINT'-------------------------------'



            PRINT'------------------------------------------------------'
            SET  @START_TIME=GETDATE();
            TRUNCATE TABLE silver.erp_CUST_AZ12;
            INSERT INTO silver.erp_CUST_AZ12
            (CID ,
            BDATE ,
            GEN) 
            SELECT
            CASE WHEN CID LIKE 'NAS%' 
                 THEN SUBSTRING(CID,4,LEN(CID))
                 ELSE CID
            END CID,
            CASE WHEN  BDATE < '1925' OR BDATE >GETDATE() 
                 THEN NULL
                 ELSE CAST(BDATE AS DATE)
            END BDATE,
            CASE WHEN TRIM(UPPER(GEN)) IN ('F','Female') THEN 'Female'
                 WHEN TRIM(UPPER(GEN)) IN ('M','Male') THEN 'Male'
                  ELSE 'Null'
            END GEN
            FROM bronze.erp_CUST_AZ12;
            SET @END_TIME=GETDATE();
            PRINT'>>>DURATION OF  silver.erp_CUST_AZ12'+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR) + ' SECONDS'

            PRINT'------------------------------------------------------'
            SET  @START_TIME=GETDATE();
            TRUNCATE TABLE silver.erp_LOC_A101;
            INSERT INTO silver.erp_LOC_A101
            (
            CID ,
            CNTRY)

            SELECT 
            REPLACE(CID,'-','') AS CID,
            CASE WHEN TRIM(CNTRY) IN ('DE') THEN 'Germany'
                 WHEN TRIM(CNTRY) IN ('USA','US') THEN 'United States'
                 WHEN TRIM(CNTRY) IN ('') THEN 'NULL'
                 ELSE TRIM(CNTRY)
            END CNTRY
            FROM bronze.erp_LOC_A101;
            SET @END_TIME=GETDATE();
            PRINT'>>>DURATION OF silver.erp_LOC_A101 '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR) + ' SECONDS'

            PRINT'------------------------------------------------------'
            SET  @START_TIME=GETDATE();
            TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
            INSERT INTO silver.erp_PX_CAT_G1V2
            (ID,
            CAT,
            SUBCAT ,
            MAINTENANCE )
            SELECT  
            REPLACE(ID,'-','_') AS ID,
            CAT,
            SUBCAT ,
            MAINTENANCE 
            FROM bronze.erp_PX_CAT_G1V2;
            SET @END_TIME=GETDATE();
            SET @BATCHEND_TIME=GETDATE();
            PRINT'>>>DURATION OF silver.erp_PX_CAT_G1V2 '+ CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR) + ' SECONDS'
            PRINT'-------------------------------'
            PRINT'END OF SILVER ERP TABLE'
            PRINT'-------------------------------'
            PRINT'=============================='
            PRINT'END OF SILVER LAYER'
            PRINT'=============================='
            PRINT'>>>DURATION OF SILVER LAYER IS '+ CAST(DATEDIFF(SECOND,@BATCHSTART_TIME,@BATCHEND_TIME)AS NVARCHAR) + ' SECONDS'

    END TRY
    BEGIN CATCH
     PRINT'ERROR MESSAGE OCCURE DURIN SILVER LAYER'
     PRINT'ERROR MESSAGE ' + CAST(ERROR_MESSAGE() AS NVARCHAR);
     PRINT'ERROR NUMBER ' + CAST(ERROR_NUMBER() AS NVARCHAR);
     PRINT'ERROR STATE ' + CAST(ERROR_STATE() AS NVARCHAR);
    END CATCH

END



============ DDL Gold Layer ===============


IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_id pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO

```
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

## About Me
Rithika R
📌 Data Analyst | SQL | Python | Tableau | Power BI | Excel
🔗 ![LinkedIn](https://www.linkedin.com/in/rithika-ramalingam-r-02714b244/) • ![GitHub](https://github.com/settings/profile)
