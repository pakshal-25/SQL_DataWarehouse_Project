IF OBJECT_ID('gold.dim_customers','V') is not NULL
    DROP VIEW gold.dim_customers;
GO 


CREATE VIEW gold.dim_customers as
SELECT 
    row_number() over(order by cst_id) as customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    la.cntry as country,
    ci.cst_material_status as martial_status,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
         ELSE COALESCE(ca.gen,'n/a')
    END as gender,
    ci.cst_create_date as create_date,
    ca.bdate as birtdate
    
FROM 
    silver.crm_cust_info ci 
LEFT JOIN 
    silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid 
LEFT JOIN 
    silver.erp_loc_a101 la 
ON ci.cst_key = la.cid
GO

IF OBJECT_ID('gold.dim_products','V') is not NULL
    DROP VIEW gold.dim_products;
GO 


CREATE VIEW gold.dim_products as
SELECT 
    row_number() over(order by pn.prd_start_dt,pn.prd_key) as product_key,
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pn.cat_id as category_id,
    pc.cat as category,
    pc.subcat as subcategory_id,
    pc.maintenance,
    pn.prd_cost as cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
    
FROM 
    silver.crm_prd_info pn 
LEFT JOIN 
    silver.erp_px_cat_g1v2 pc 
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- FILTER OUT HISTORICAL DATA
GO 

IF OBJECT_ID('gold.fact_sales','V') is not NULL
    DROP VIEW gold.fact_sales;
GO 

CREATE VIEW gold.fact_sales as 
SELECT 
        sd.sls_ord_num as order_number,
        pr.product_key,
        cu.customer_key,
        sd.sls_order_dt as order_date,
        sd.sls_ship_dt as shipping_date,
        sd.sls_sales as sales_amount,
        sd.sls_quantity as quantity,
        sd.sls_price as price
    FROM 
        silver.crm_sales_details sd 
    LEFT JOIN gold.dim_products pr 
    on sd.sls_prd_key =  pr.product_number
    LEFT JOIN gold.dim_customers cu 
    on sd.sls_cust_id = cu.customer_id
    GO 

--foreign key integrity
-- select * from gold.fact_sales f
-- left join 
-- gold.dim_customers c 
-- on c.customer_key =  f.customer_key 
-- left join gold.dim_products p 
-- on p.product_key = f.product_key
-- where c.customer_key is null



-- SELECT 
--     ci.cst_gndr,
--     ca.gen,
--     CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
--          ELSE COALESCE(ca.gen,'n/a')
--     END as new_gen
-- FROM silver.crm_cust_info ci 
-- LEFT JOIN  silver.erp_cust_az12 ca 
-- ON ci.cst_key = ca.cid 
-- LEFT JOIN silver.erp_loc_a101 la 
-- ON ci.cst_key = la.cid 
-- ORDER BY 1,2
