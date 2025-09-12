/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE or ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '====================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '====================================';

		PRINT '*************************************';
		PRINT 'LOADING CRM TABLE';
		PRINT '*************************************';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\bermo\OneDrive\Documents\Beber\Learning\Data\SQL Course Materials\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		--SELECT * FROM bronze.crm_cust_info;
		SELECT COUNT(*) FROM bronze.crm_cust_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
		Print '>>*******************************<<';

		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\bermo\OneDrive\Documents\Beber\Learning\Data\SQL Course Materials\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		--SELECT * FROM bronze.crm_prd_info;
		SELECT COUNT(*) FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
		Print '>>*******************************<<';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\bermo\OneDrive\Documents\Beber\Learning\Data\SQL Course Materials\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		--SELECT * FROM bronze.crm_sales_details;
		SELECT COUNT(*) FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
		Print '>>*******************************<<';

		PRINT '*************************************';
		PRINT 'LOADING ERP TABLE';
		PRINT '*************************************';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\bermo\OneDrive\Documents\Beber\Learning\Data\SQL Course Materials\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		--SELECT * FROM bronze.erp_cust_az12;
		SELECT COUNT(*) FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
		Print '>>*******************************<<';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\bermo\OneDrive\Documents\Beber\Learning\Data\SQL Course Materials\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		/*SELECT * FROM bronze.erp_loc_a101;*/
		SELECT COUNT(*) FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
		Print '>>*******************************<<';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\bermo\OneDrive\Documents\Beber\Learning\Data\SQL Course Materials\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		/*SELECT * FROM bronze.erp_px_cat_g1v2;*/
		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
		Print '>>*******************************<<';

		SET @batch_end_time = GETDATE();
		PRINT '==============================';
		PRINT 'Loading Bronze Layer Is Completed';
		PRINT '- Total Load Duration:' + Cast(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '==============================';
	END TRY
	BEGIN CATCH
		PRINT '===================================';
		PRINT 'Error Occured During Loading Bronze Layer';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===================================';
	END CATCH
END;
