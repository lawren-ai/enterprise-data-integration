ALTER TABLE stg_crm_customers
ALTER COLUMN load_id TYPE BIGINT;

ALTER TABLE stg_products
ALTER COLUMN load_id TYPE BIGINT;

ALTER TABLE stg_ecom_transactions
ALTER COLUMN load_id TYPE BIGINT;

ALTER TABLE stg_ecom_transaction_items
ALTER COLUMN load_id TYPE BIGINT;

ALTER TABLE stg_marketing_campaigns
ALTER COLUMN load_id TYPE BIGINT;

ALTER TABLE stg_campaign_responses
ALTER COLUMN load_id TYPE BIGINT;

ALTER TABLE stg_audit_log
ALTER COLUMN load_id TYPE BIGINT;
