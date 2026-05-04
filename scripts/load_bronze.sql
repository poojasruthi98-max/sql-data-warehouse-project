/*
======================================================================================
Stored Procedure: Load Bronze Layer (Source - > Bronze)
======================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
  None.
 This stored procedure does not accept any parameters or return any values.

Usage Example:
   EXEC bronze.load_bronze;
======================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time   DATETIME;

    DECLARE @batch_start_time DATETIME,
            @batch_end_time   DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '======================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '======================================================';

        /* ===================== CRM TABLES ===================== */
        PRINT '------------------- Loading CRM Tables -------------------';

        -- CRM CUSTOMER
        SET @start_time = GETDATE();

        PRINT '>> Truncating: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Loading: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/data/Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT CONCAT('>> Duration: ', DATEDIFF(SECOND, @start_time, @end_time), ' seconds');
        PRINT '------------------------------------------------------';


        -- CRM PRODUCT
        SET @start_time = GETDATE();

        PRINT '>> Truncating: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Loading: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/data/Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT CONCAT('>> Duration: ', DATEDIFF(SECOND, @start_time, @end_time), ' seconds');
        PRINT '------------------------------------------------------';


        -- CRM SALES
        SET @start_time = GETDATE();

        PRINT '>> Truncating: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Loading: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/data/Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT CONCAT('>> Duration: ', DATEDIFF(SECOND, @start_time, @end_time), ' seconds');
        PRINT '------------------------------------------------------';


        /* ===================== ERP TABLES ===================== */
        PRINT '------------------- Loading ERP Tables -------------------';

        -- ERP LOCATION
        SET @start_time = GETDATE();

        PRINT '>> Truncating: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Loading: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/data/Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT CONCAT('>> Duration: ', DATEDIFF(SECOND, @start_time, @end_time), ' seconds');
        PRINT '------------------------------------------------------';


        -- ERP CUSTOMER
        SET @start_time = GETDATE();

        PRINT '>> Truncating: bronze.erp_cust_a1z12';
        TRUNCATE TABLE bronze.erp_cust_a1z12;

        PRINT '>> Loading: bronze.erp_cust_a1z12';
        BULK INSERT bronze.erp_cust_a1z12
        FROM '/data/Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_erp/cust_a1z12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT CONCAT('>> Duration: ', DATEDIFF(SECOND, @start_time, @end_time), ' seconds');
        PRINT '------------------------------------------------------';


        -- ERP CATEGORY
        SET @start_time = GETDATE();

        PRINT '>> Truncating: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Loading: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/data/Data_Warehouse_Project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT CONCAT('>> Duration: ', DATEDIFF(SECOND, @start_time, @end_time), ' seconds');
        PRINT '------------------------------------------------------';

        SET @batch_end_time = GETDATE();
        PRINT '======================================================'
        PRINT 'Loading Bronze Layer is Completed'
        PRINT '    - Total Load Duration:  ' + CAST(DATEDIFF(SECOND, @batch_end_time, @batch_end_time) AS NVARCHAR) + 'seconds';
        PRINT '======================================================'


        PRINT '✅ Bronze Layer Loaded Successfully';

    END TRY

    BEGIN CATCH
        PRINT '======================================================';
        PRINT '❌ ERROR OCCURRED DURING BRONZE LOAD';

        PRINT CONCAT('Error Message: ', ERROR_MESSAGE());
        PRINT CONCAT('Error Number : ', ERROR_NUMBER());
        PRINT CONCAT('Error State  : ', ERROR_STATE());

        PRINT '======================================================';
    END CATCH

END;



