
/*
==========================================================================================================
Stored Procedure: load Bronze layer (source -> Bronze)
==========================================================================================================
Script purpose:
    This stored procedure loads data into bronze schema from external csv files.
    it performs the following action
    - Truncates the bronze tables before loading data
    - Used the 'BULK INSERT' command to load data from csv files to bronze tables

Parameters:
    None
    This stored procedure does not accept any parameters or returns any values

usage examples
    EXEC bronze.load_bronze;
==========================================================================================================
*/


EXEC bronze.load_bronze;
CREATE OR ALTER PROCEDURE bronze.load_bronze as 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=====================================================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '=====================================================';
        PRINT '-------------------------------------------------------';
        PRINT 'LOADING CRM TABLES';
        PRINT '-------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>>Truncating the table bronze.crm_cust_info ';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>>Inserting the data into bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/cust_info.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '>> ------------------'
        
        SET @start_time = GETDATE()
        PRINT '>>Truncating the table bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info
        PRINT '>>Inserting the data into bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/data/prd_info.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>>LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
        PRINT '>>---------------------'
        
        SET @start_time = GETDATE();
        PRINT '>>Truncating the table bronze.crm_sales_details ';
        TRUNCATE TABLE bronze.crm_sales_details
        PRINT '>>Inserting the data into bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/sales_details.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time = GETDATE();
        PRINT '>> LOADING DURATION '+ CAST(DATEDIFF(second, @start_time,@end_time) as NVARCHAR) + 'seconds';
        PRINT '>> ------------------'
    
        PRINT '-------------------------------------------------------';
        PRINT 'LOADING ERP TABLES';
        PRINT '-------------------------------------------------------';

        
        SET @start_time = GETDATE();
        PRINT '>>Truncating the table bronze.erp_cust_az12 ';
        TRUNCATE TABLE bronze.erp_cust_az12
        PRINT '>>Inserting the data into bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/data/cust_az12.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time = GETDATE();
        PRINT '>>LOADING DURATION' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
        PRINT '>> ------------------';


        
        SET @start_time = GETDATE();
        PRINT '>>Truncating the table bronze.erp_loc_a101 ';
        TRUNCATE TABLE bronze.erp_loc_a101
        PRINT '>>Inserting the data into bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/data/loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+ 'seconds';
        PRINT'-----------';

        SET @start_time = GETDATE()
        PRINT '>>Truncating the table bronze.erp_px_cat_g1v2 ';
        TRUNCATE TABLE bronze.erp_loc_a101
        PRINT '>>Inserting the data into bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/data/px_cat_g1v2.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE() 
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '>>-----------------';

        SET @batch_end_time = GETDATE();
        PRINT '==============================';
        PRINT 'LOADING BRONZE LAYER IS COMPLETED';
        PRINT 'TOTAL DURATION TIME: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) as NVARCHAR) + 'seconds';
        PRINT '========================='
    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END

