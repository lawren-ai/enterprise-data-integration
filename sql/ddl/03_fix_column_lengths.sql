-- =====================================================
-- FIX COLUMN LENGTHS
-- Adjust VARCHAR lengths to accommodate actual data
-- =====================================================

-- Fix phone_number column in dim_customer (currently VARCHAR(20), needs to be longer)
ALTER TABLE dim_customer 
ALTER COLUMN phone_number TYPE VARCHAR(50);

-- Fix phone_number column in staging table too
ALTER TABLE stg_crm_customers 
ALTER COLUMN phone_number TYPE VARCHAR(50);

-- Verify the changes
SELECT 
    column_name, 
    data_type, 
    character_maximum_length
FROM information_schema.columns
WHERE table_name = 'dim_customer'
  AND column_name = 'phone_number';