/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    Actions Performed:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql 
AS $$
DECLARE 
    batch_start TIMESTAMP;
    batch_end TIMESTAMP;
    table_start_time TIMESTAMP;
    table_end_time TIMESTAMP;
    error_message TEXT;
    error_state TEXT;
BEGIN
    RAISE NOTICE '>> Starting Bronze Layer Load...';
    batch_start := clock_timestamp();
    
    BEGIN
        ---------------------------
        -- bronze.crm_cust_info
        ---------------------------

        RAISE NOTICE '>> Truncating: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        
        RAISE NOTICE '>> Inserting into: bronze.crm_cust_info';
        
        table_start_time := clock_timestamp();
        
        COPY bronze.crm_cust_info FROM '/tmp/cust_info.csv' WITH CSV HEADER;
        
        table_end_time := clock_timestamp();
        RAISE NOTICE 'Loaded bronze.crm_cust_info in % seconds', EXTRACT(SECOND FROM table_end - table_start);

        ---------------------------
        -- bronze.crm_prd_info
        ---------------------------

        RAISE NOTICE '>> Truncating: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        
        RAISE NOTICE '>> Inserting into: bronze.crm_prd_info';
        
        table_start_time := clock_timestamp();
        
        COPY bronze.crm_prd_info FROM '/tmp/prd_info.csv' WITH CSV HEADER;
        
        table_end_time := clock_timestamp();
        RAISE NOTICE 'Loaded bronze.crm_prd_info in % seconds', EXTRACT(SECOND FROM table_end_time - table_start_time_time);

        ---------------------------
        -- bronze.crm_sales_details
        ---------------------------

        RAISE NOTICE '>> Truncating: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;        
        
        RAISE NOTICE '>> Inserting into: bronze.crm_sales_details';    
        
        table_start_time := clock_timestamp();
        
        COPY bronze.crm_sales_details FROM '/tmp/sales_details.csv' WITH CSV HEADER;
        
        table_end_time := clock_timestamp();
        RAISE NOTICE '  Loaded bronze.crm_sales_details in % seconds', EXTRACT(SECOND FROM table_end_time - table_start_time_time);

        ---------------------------
        -- bronze.erp_cust_az12
        ---------------------------
        
        RAISE NOTICE '>> Truncating: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;        
        
        table_start_time := clock_timestamp();
        
        RAISE NOTICE '>> Inserting into: bronze.erp_cust_az12';
        
        COPY bronze.erp_cust_az12 FROM '/tmp/CUST_AZ12.csv' WITH CSV HEADER;
        
        table_end_time := clock_timestamp();
        RAISE NOTICE '  Loaded bronze.erp_cust_az12 in % seconds', EXTRACT(SECOND FROM table_end_time - table_start_time_time);

        ---------------------------
        -- bronze.erp_loc_a101
        ---------------------------
        
        RAISE NOTICE '>> Truncating: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISE NOTICE '>> Inserting into: bronze.erp_loc_a101';
        
        table_start_time := clock_timestamp();
        
        COPY bronze.erp_loc_a101 FROM '/tmp/LOC_A101.csv' WITH CSV HEADER;
        
        table_end_time := clock_timestamp();
        RAISE NOTICE '  Loaded bronze.erp_loc_a101 in % seconds', EXTRACT(SECOND FROM table_end_time - table_start_time_time);

        ---------------------------
        -- bronze.erp_px_cat_g1v2
        ---------------------------

        RAISE NOTICE '>> Truncating: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE '>> Inserting into: bronze.erp_px_cat_g1v2';        table_start_time := clock_timestamp();
        
        COPY bronze.erp_px_cat_g1v2 FROM '/tmp/PX_CAT_G1V2.csv' WITH CSV HEADER;
        
        table_end_time := clock_timestamp();
        RAISE NOTICE '  Loaded bronze.erp_px_cat_g1v2 in % seconds', EXTRACT(SECOND FROM table_end_time - table_start_time);
    
        ---------------------------
        -- Batch End
        ---------------------------    
        batch_end := clock_timestamp();
        RAISE NOTICE 'Total Batch Load Time: % seconds', ROUND(EXTRACT(EPOCH FROM (batch_end - batch_start))::NUMERIC, 2);

    EXCEPTION
        WHEN OTHERS THEN
            error_message := SQLERRM;  -- Captures error message
            error_state := SQLSTATE;   -- Captures error code
            RAISE NOTICE '!! ERROR OCCURRED !!';
            RAISE NOTICE 'Error Message: %', error_message;
            RAISE NOTICE 'Error State: %', error_state;
END;
END $$;
