
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @batch_start_time DATETIME = GETDATE();
    DECLARE @start_time DATETIME, @end_time DATETIME, @rows_affected INT;

    PRINT '=================================================';
    PRINT '                Loading Bronze Layer             ';
    PRINT '=================================================';

    BEGIN TRY
        -- CRM Customer Info
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data into: bronze.crm_cust_info';

        BULK INSERT bronze.crm_cust_info
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);

        SET @end_time = GETDATE();
        SELECT @rows_affected = COUNT(*) FROM bronze.crm_cust_info;
        PRINT '(' + CAST(@rows_affected AS NVARCHAR(10)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '---------------------------------------------------';

        -- CRM Product Info
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data into: bronze.crm_prd_info';

        BULK INSERT bronze.crm_prd_info
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);

        SET @end_time = GETDATE();
        SELECT @rows_affected = COUNT(*) FROM bronze.crm_prd_info;
        PRINT '(' + CAST(@rows_affected AS NVARCHAR(10)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '---------------------------------------------------';

        -- CRM Sales Details
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data into: bronze.crm_sales_details';

        BULK INSERT bronze.crm_sales_details
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);

        SET @end_time = GETDATE();
        SELECT @rows_affected = COUNT(*) FROM bronze.crm_sales_details;
        PRINT '(' + CAST(@rows_affected AS NVARCHAR(10)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '---------------------------------------------------';

        -- ERP Location Info
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data into: bronze.erp_loc_a101';

        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);

        SET @end_time = GETDATE();
        SELECT @rows_affected = COUNT(*) FROM bronze.erp_loc_a101;
        PRINT '(' + CAST(@rows_affected AS NVARCHAR(10)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '---------------------------------------------------';

        -- ERP Customer Info
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data into: bronze.erp_cust_az12';

        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);

        SET @end_time = GETDATE();
        SELECT @rows_affected = COUNT(*) FROM bronze.erp_cust_az12;
        PRINT '(' + CAST(@rows_affected AS NVARCHAR(10)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '---------------------------------------------------';

        -- ERP Product Category
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data into: bronze.erp_px_cat_g1v2';

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.CSV'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);

        SET @end_time = GETDATE();
        SELECT @rows_affected = COUNT(*) FROM bronze.erp_px_cat_g1v2;
        PRINT '(' + CAST(@rows_affected AS NVARCHAR(10)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
        PRINT '---------------------------------------------------';

        -- Capture End Time
        DECLARE @batch_end_time DATETIME = GETDATE();
        PRINT '========================================';
        PRINT '         Loading Bronze Layer Completed ';
        PRINT '========================================';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' seconds';

    END TRY
    BEGIN CATCH
        PRINT '========================================';
        PRINT '  ERROR OCCURRED DURING LOADING BRONZE LAYER  ';
        PRINT '========================================';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(10));
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10));
    END CATCH
END;
GO
exec bronze.load_bronze;
