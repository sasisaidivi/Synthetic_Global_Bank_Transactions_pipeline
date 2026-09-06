CREATE OR REPLACE MATERIALIZED VIEW `sasisaidivi.auth_views_synthetic.total_deposit_credits`
AS
SELECT
    SUM(deposit) AS total_deposits,
    SUM(credit) AS total_credit
FROM `sasisaidivi.refine_synthetic.clients`;