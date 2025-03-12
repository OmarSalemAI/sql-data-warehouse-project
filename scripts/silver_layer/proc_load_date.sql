/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
		PRINT '==================================';
		PRINT 'Loading Silver Layer';
		PRINT '==================================';
		PRINT '-----------------------------------';
		PRINT 'Loading CRM Data';
		PRINT '-----------------------------------';
	BEGIN TRY
		SET @start_time = GETDATE()
		SET @batch_start_time = GETDATE()
		PRINT '>> Trancate Table silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting Data Into silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gnder,
			cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			-- Remove Unwanted Spaces
			TRIM(cst_firstname) cst_firstname,
			TRIM(cst_lastname) cst_lastname,
			-- Data Standrazation & Normalization & Handling Missing Values
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 ELSE 'n/a'
			END cst_marital_status,

			CASE WHEN UPPER(TRIM(cst_gnder)) = 'M' THEN 'Male'
				 WHEN UPPER(TRIM(cst_gnder)) = 'F' THEN 'Female'
				 ELSE 'n/a'
			END cst_gnder,

			cst_create_date

		FROM
		(
			-- remove duplicates and null values
			SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS recent_update
			FROM bronze.crm_cust_info
		) t WHERE recent_update = 1 AND cst_id IS NOT NULL
		SET @end_time = GETDATE()
		PRINT '>> THE WHOLE TIME FOR LOADING IS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT '-------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Trancate Table silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Inserting Data Into silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			prd_cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt

		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'M' THEN 'Mountain'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line,
			prd_start_dt,
			DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE()
		PRINT '>> THE WHOLE TIME FOR LOADING IS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT '-------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Trancate Table silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Inserting Data Into silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			-- handling incorrect date and casting it to date
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS nvarchar) AS DATE)
			END sls_order_dt,
			-- casting it to date
			CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE) sls_ship_dt,
			CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE) sls_due_dt,
			-- handling invalid sls_sales by using original formula
			CASE WHEN sls_sales != sls_quantity * ABS(sls_price) OR sls_sales IS NULL OR sls_sales <= 0
			THEN  sls_quantity * ABS(sls_price)
			ELSE sls_sales
			END sls_sales,

			sls_quantity,
			-- derived from orignal column 
			CASE WHEN sls_price <= 0 OR sls_price IS NULL
			THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
			ELSE sls_price
			END sls_price

		FROM bronze.crm_sales_details
		SET @end_time = GETDATE()
		PRINT '>> THE WHOLE TIME FOR LOADING IS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT '-------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Trancate Table silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>> Inserting Data Into silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT 
			-- Handling column cid to make it suitable for joining 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
				 ELSE cid
			END cid,
			-- Ensure that we clean future bdate
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END bdate,
			-- Date Standriazation & Normalization
			CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				 WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				 ELSE 'n/a'
			END gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE()
		PRINT '>> THE WHOLE TIME FOR LOADING IS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT '-------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Trancate Table silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> Inserting Data Into silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)
		SELECT 
			REPLACE(cid, '-', '') AS cid,
			CASE WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
				 WHEN TRIM(cntry) = '' THEN NULL
				 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 ELSE cntry
				 END cntry

		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE()
		PRINT '>> THE WHOLE TIME FOR LOADING IS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT '-------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Trancate Table silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>> Inserting Data Into silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT * FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE()
		PRINT '>> THE WHOLE TIME FOR LOADING IS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT '-------------------------------------------------'

		SET @batch_end_time = GETDATE()
		PRINT '>> THE WHOLE TIME FOR LOADING IS: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' SECONDS'
		END TRY
	BEGIN CATCH
	PRINT '============================================'
	PRINT 'THERE IS AN ERROR OCCUR DURING LOADING'
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE()
	PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR)
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR)
	PRINT '============================================'
		END CATCH
END
