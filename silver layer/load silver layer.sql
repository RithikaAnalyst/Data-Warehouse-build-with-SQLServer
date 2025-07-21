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


