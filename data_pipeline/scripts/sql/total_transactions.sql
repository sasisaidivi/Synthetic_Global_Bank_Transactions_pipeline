CREATE OR REPLACE MATERIALIZED VIEW `sasisaidivi.auth_views_synthetic.total_transactions`
AS
SELECT
    SUM(amount) AS total_transaction_amount
FROM `sasisaidivi.refine_synthetic.transactions`;

