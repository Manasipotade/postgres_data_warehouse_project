/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
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
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    table_start_time TIMESTAMP;
    table_end_time TIMESTAMP;
    error_message TEXT;
    error_state TEXT;
BEGIN
    BEGIN
        RAISE NOTICE '>> Starting Batch Load for All Tables';
        batch_start_time := clock_timestamp();  -- Capture batch start time

        -- Load bronze.crm_cust_info
        RAISE NOTICE '>> Truncating and Loading: bronze.crm_cust_info';
        table_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info FROM '/tmp/cust_info.csv' WITH CSV HEADER;
        table_end_time := clock_timestamp();
        RAISE NOTICE '>> Table Load Time: bronze.crm_cust_info - % seconds', EXTRACT(EPOCH FROM (table_end_time - table_start_time));

        -- Load bronze.crm_prd_info
        RAISE NOTICE '>> Truncating and Loading: bronze.crm_prd_info';
        table_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info FROM '/tmp/prd_info.csv' WITH CSV HEADER;
        table_end_time := clock_timestamp();
        RAISE NOTICE '>> Table Load Time: bronze.crm_prd_info - % seconds', EXTRACT(EPOCH FROM (table_end_time - table_start_time));

        -- Load bronze.crm_sales_details
        RAISE NOTICE '>> Truncating and Loading: bronze.crm_sales_details';
        table_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details FROM '/tmp/sales_details.csv' WITH CSV HEADER;
        table_end_time := clock_timestamp();
        RAISE NOTICE '>> Table Load Time: bronze.crm_sales_details - % seconds', EXTRACT(EPOCH FROM (table_end_time - table_start_time));

        -- Load bronze.erp_cust_az12
        RAISE NOTICE '>> Truncating and Loading: bronze.erp_cust_az12';
        table_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12 FROM '/tmp/CUST_AZ12.csv' WITH CSV HEADER;
        table_end_time := clock_timestamp();
        RAISE NOTICE '>> Table Load Time: bronze.erp_cust_az12 - % seconds', EXTRACT(EPOCH FROM (table_end_time - table_start_time));

        -- Load bronze.loc_a101
        RAISE NOTICE '>> Truncating and Loading: bronze.loc_a101';
        table_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.loc_a101;
        COPY bronze.loc_a101 FROM '/tmp/LOC_A101.csv' WITH CSV HEADER;
        table_end_time := clock_timestamp();
        RAISE NOTICE '>> Table Load Time: bronze.loc_a101 - % seconds', EXTRACT(EPOCH FROM (table_end_time - table_start_time));

        -- Load bronze.erp_px_cat_g1v2
        RAISE NOTICE '>> Truncating and Loading: bronze.erp_px_cat_g1v2';
        table_start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2 FROM '/tmp/PX_CAT_G1V2.csv' WITH CSV HEADER;
        table_end_time := clock_timestamp();
        RAISE NOTICE '>> Table Load Time: bronze.erp_px_cat_g1v2 - % seconds', EXTRACT(EPOCH FROM (table_end_time - table_start_time));

        -- Capture end time and print total batch time
        batch_end_time := clock_timestamp();
        RAISE NOTICE '>> Total Batch Load Time: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));

    EXCEPTION
        WHEN OTHERS THEN
            error_message := SQLERRM;  -- Captures error message
            error_state := SQLSTATE;   -- Captures error code
            RAISE NOTICE '!! ERROR OCCURRED !!';
            RAISE NOTICE 'Error Message: %', error_message;
            RAISE NOTICE 'Error State: %', error_state;
END;
END $$;
