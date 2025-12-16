/* Stored Procedure : Load Silver Layer(Bronze -> Silver)
script purpose:
      This stored Procedure performs ETL process to populate 'silver' schema tablers from the 'bronze' schema
    Action performed:
    Truncate silver tables
    Inserts Transformed and cleaned data from bronze into silver tables

Parameters:
      None.
      this stored procedure does not accept parameters or retrurn any values.
usage 
    EXEC silver.load_silver
*/
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=======================================================================';
        PRINT 'LOADING SILVER TABLE';
        PRINT '=======================================================================';

        PRINT '=======================================================================';
        PRINT 'LOADING CRM TABLE';
        PRINT '=======================================================================';

        --LOADING silver.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>>INSERTING Data Into :silver.crm_cust_info'
        INSERT INTO silver.crm_cust_info
        (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_date)
    select
        cst_id,
        cst_key,
        TRIM(cst_firstname) as cst_firstname,
        TRIM(cst_lastname) as cst_lastname,
        CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' then 'Single'
                    WHEN UPPER(TRIM(cst_material_status)) ='M' then 'Married'
                    ELSE 'N/a'
                END cst_material_status,
        CASE WHEN UPPER(TRIM(cst_gndr)) ='F' then 'Female'
                    WHEN UPPER(TRIM(cst_gndr)) ='M' then 'Male'
                    ELSE 'N/A'
                END cst_gndr,
        cst_create_date
    from (
                        select
            *,
            row_number()over(partition by cst_id order by cst_create_date desc) as rnk
        from bronze.crm_cust_info
        where cst_id is not null
                        ) t
    where rnk = 1 

            SET @end_time = GETDATE();
            PRINT'LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds'
            PRINT '-------------------------';

        SET  @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info
        PRINT '>> Inserting Data into: silver.crm_prd_info'
        INSERT INTO silver.crm_prd_info
        (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
        )


    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
        SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
        prd_nm,
        ISNULL(prd_cost,0) AS prd_cost,
        CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN  'other_sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
        CAST(prd_start_dt as DATE) AS prd_start_dt,
        CAST(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1  AS DATE) AS prd_end_dt
    from bronze.crm_prd_info

        SET @end_time = GETDATE();
        PRINT'LOAD DURATION '+ CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds'


        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.crm_sales_details';
        --Loading crm_sales_details
        TRUNCATE TABLE silver.crm_sales_details
        PRINT '>> Inserting data into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details
        (
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

        -- Clean order date
        CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,

        -- Clean ship date
        CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,

        -- Clean due date
        CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,


        CASE 
                WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales!= sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales 
            END AS sls_sales,


        sls_quantity,


        CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price 
            END AS sls_price
    FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT'LOAD DURATION'+ CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds'


        
        PRINT '=======================================================================';
        PRINT 'LOADING CRM TABLE';
        PRINT '=======================================================================';

        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12
        PRINT '>> Inserting data into: silver.erp_cust_az12';
        INSERT INTO SILVER.erp_cust_az12
        (cid,bdate,gen)
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
                ELSE cid 
            END AS cid,
        CASE WHEN bdate > GETDATE() THEN NULL
                ELSE BDATE
            END AS bdate,
        CASE 
                WHEN gen IS NULL OR LTRIM(RTRIM(gen)) = '' THEN 'n/a'

                -- Arabic → English conversions
                WHEN TRANSLATE(gen, N'مفـ', N'MF ') LIKE '%M%' THEN 'Male'
                WHEN TRANSLATE(gen, N'مفـ', N'MF ') LIKE '%F%' THEN 'Female'

                -- Standard English values
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'

                ELSE 'n/a'
            END AS gen

    FROM
        bronze.erp_cust_az12
        SET @end_time= GETDATE()
        PRINT'LOAD DURATION' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds'

        SET @start_time = GETDATE()
        PRINT '>> TRUNCATING TABLE: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101
        PRINT '>> Inserting data into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101
        (cid,cntry)
    SELECT
        replace(cid,'-','') cid,
        CASE
                WHEN cntry IS NULL
            OR LTRIM(RTRIM(
                            REPLACE(
                            REPLACE(cntry, CHAR(13), ''),
                            CHAR(10), ''
                            )
                        )) = ''
                THEN 'n/a'

                WHEN UPPER(LTRIM(RTRIM(
                        REPLACE(
                        REPLACE(cntry, CHAR(13), ''),
                        CHAR(10), ''
                        )
                    ))) = 'DE'
                THEN 'GERMANY'

                WHEN UPPER(LTRIM(RTRIM(
                        REPLACE(
                        REPLACE(cntry, CHAR(13), ''),
                        CHAR(10), ''
                        )
                    ))) IN ('US','USA')
                THEN 'United States'

                ELSE LTRIM(RTRIM(
                        REPLACE(
                        REPLACE(cntry, CHAR(13), ''),
                        CHAR(10), ''
                        )
                    ))
            END AS cntry
    FROM
        bronze.erp_loc_a101
        SET @end_time= GETDATE()
        PRINT 'LOAD DURATION' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds'
        
        SET @start_time = GETDATE()

        PRINT '>> TRUNCATING TABLE: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2
        PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2
        (id,cat,subcat,maintenance)
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM
        bronze.erp_px_cat_g1v2
        SET @end_time= GETDATE()
        PRINT 'LOAD DURATION' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds'
    END TRY 
    BEGIN CATCH
        PRINT'===================================='
        PRINT 'ERROR OCCURED DURING LAODING BRONZE LAYER'
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERRRO MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT '===================================='
    END CATCH
END
