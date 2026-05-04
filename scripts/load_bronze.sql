
/*
=================================================================================
DDL Script: Create Bronze Tables
=================================================================================
Script Purpose:
   This script creates tables in the 'bronze' schema, dropping existing tables
   If they already exist.
   Run this script to re-define the DDL structure of 'bronze' tables
=================================================================================
*/

/* =========================================================
   BRONZE LAYER TABLE CREATION
   Purpose: Create raw ingestion tables for CRM and ERP data
   ========================================================= */

------------------------------------------------------------
-- CRM TABLES
------------------------------------------------------------

-- CUSTOMER INFO
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO


-- PRODUCT INFO
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id        INT,
    prd_key       NVARCHAR(50),
    prd_nm        NVARCHAR(50),
    prd_cost      INT,
    prd_line      NVARCHAR(50),
    prd_start_dt  DATETIME,
    prd_end_dt    DATETIME
);
GO


-- SALES DETAILS
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num   NVARCHAR(50),
    sls_prd_key   NVARCHAR(50),
    sls_cust_id   INT,
    sls_order_dt  INT,
    sls_ship_dt   INT,
    sls_due_dt    INT,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT
);
GO


------------------------------------------------------------
-- ERP TABLES
------------------------------------------------------------

-- LOCATION
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO


-- CUSTOMER
IF OBJECT_ID('bronze.erp_cust_a1z12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_a1z12;
GO

CREATE TABLE bronze.erp_cust_a1z12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO


-- PRODUCT CATEGORY
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO

EXEC bronze.load_bronze

-- Formatted Version

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time   DATETIME;

    DECLARE @batch_start_time DATETIME,
            @batch_end_time   DATETIME;

    BEGIN TRY

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



