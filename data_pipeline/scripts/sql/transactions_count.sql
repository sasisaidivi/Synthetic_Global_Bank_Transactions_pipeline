CREATE OR REPLACE MATERIALIZED VIEW `sasisaidivi.auth_views_synthetic.transactions_count`
AS
SELECT
    COUNT(*) AS total_transaction_count
FROM `sasisaidivi.refine_synthetic.transactions`;