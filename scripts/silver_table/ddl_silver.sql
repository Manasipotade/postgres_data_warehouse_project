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


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE 
    batch_start TIMESTAMP;
    batch_end TIMESTAMP;
    table_start TIMESTAMP;
    table_end TIMESTAMP;
BEGIN
    RAISE NOTICE '>> Starting Silver Layer Load...';
    batch_start := clock_timestamp();

    ---------------------------
    -- silver.crm_cust_info
    ---------------------------
    RAISE NOTICE '>> Truncating: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE '>> Inserting into: silver.crm_cust_info';
    table_start := clock_timestamp();

    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'n/a' 
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    table_end := clock_timestamp();
    RAISE NOTICE 'Loaded silver.crm_cust_info in % seconds', EXTRACT(SECOND FROM table_end - table_start);

    ---------------------------
    -- silver.crm_prd_info
    ---------------------------
    RAISE NOTICE '>> Truncating: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE '>> Inserting into: silver.crm_prd_info';
    table_start := clock_timestamp();

    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm,
        prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key FROM 7 FOR LENGTH(prd_key)),
        prd_nm,
        COALESCE(prd_cost, 0),
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'R' THEN 'Road'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
    FROM bronze.crm_prd_info;

    table_end := clock_timestamp();
    RAISE NOTICE '  Loaded silver.crm_prd_info in % seconds', EXTRACT(SECOND FROM table_end - table_start);

    ---------------------------
    -- silver.crm_sales_details
    ---------------------------
    RAISE NOTICE '>> Truncating: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE '>> Inserting into: silver.crm_sales_details';
    table_start := clock_timestamp();

    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt,
        sls_sales, sls_quantity, sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
             ELSE NULLIF(sls_order_dt, 0)::TEXT::DATE END,
        CASE WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
             ELSE NULLIF(sls_ship_dt, 0)::TEXT::DATE END,
        CASE WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
             ELSE NULLIF(sls_due_dt, 0)::TEXT::DATE END,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 
             OR sls_sales != sls_quantity * ABS(sls_price) 
             THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales END,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price END
    FROM bronze.crm_sales_details;

    table_end := clock_timestamp();
    RAISE NOTICE '  Loaded silver.crm_sales_details in % seconds', EXTRACT(SECOND FROM table_end - table_start);

    ---------------------------
    -- silver.erp_cust_az12
    ---------------------------
    RAISE NOTICE '>> Truncating: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE '>> Inserting into: silver.erp_cust_az12';
    table_start := clock_timestamp();

    INSERT INTO silver.erp_cust_az12 (
        cid, bdate, gen
    )
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
             ELSE cid END,
        CASE WHEN bdate > CURRENT_DATE THEN NULL
             ELSE bdate END,
        CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
             WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
             ELSE 'n/a' END
    FROM bronze.erp_cust_az12;

    table_end := clock_timestamp();
    RAISE NOTICE '  Loaded silver.erp_cust_az12 in % seconds', EXTRACT(SECOND FROM table_end - table_start);

    ---------------------------
    -- silver.erp_loc_a101
    ---------------------------
    RAISE NOTICE '>> Truncating: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE NOTICE '>> Inserting into: silver.erp_loc_a101';
    table_start := clock_timestamp();

    INSERT INTO silver.erp_loc_a101 (
        cid, cntry
    )
    SELECT
        REPLACE(cid, '-', ''),
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    table_end := clock_timestamp();
    RAISE NOTICE '  Loaded silver.erp_loc_a101 in % seconds', EXTRACT(SECOND FROM table_end - table_start);

    ---------------------------
    -- silver.erp_px_cat_g1v2
    ---------------------------
    RAISE NOTICE '>> Truncating: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting into: silver.erp_px_cat_g1v2';
    table_start := clock_timestamp();

    INSERT INTO silver.erp_px_cat_g1v2 (
        id, cat, subcat, maintenance
    )
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

    table_end := clock_timestamp();
    RAISE NOTICE '  Loaded silver.erp_px_cat_g1v2 in % seconds', EXTRACT(SECOND FROM table_end - table_start);

    ---------------------------
    -- Batch End
    ---------------------------
    batch_end := clock_timestamp();
    RAISE NOTICE ' Total Batch Load Time: % seconds', ROUND(EXTRACT(EPOCH FROM (batch_end - batch_start))::NUMERIC, 2);

END;
$$;
