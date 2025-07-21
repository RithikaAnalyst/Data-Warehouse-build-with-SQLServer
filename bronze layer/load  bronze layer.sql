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

