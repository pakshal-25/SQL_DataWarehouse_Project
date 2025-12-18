foreign key integrity
select * from gold.fact_sales f
left join 
gold.dim_customers c 
on c.customer_key =  f.customer_key 
left join gold.dim_products p 
on p.product_key = f.product_key
where c.customer_key is null



SELECT 
    ci.cst_gndr,
    ca.gen,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
         ELSE COALESCE(ca.gen,'n/a')
    END as new_gen
FROM silver.crm_cust_info ci 
LEFT JOIN  silver.erp_cust_az12 ca 
ON ci.cst_key = ca.cid 
LEFT JOIN silver.erp_loc_a101 la 
ON ci.cst_key = la.cid 
ORDER BY 1,2
