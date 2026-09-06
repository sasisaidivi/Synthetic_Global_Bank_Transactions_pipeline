CREATE OR REPLACE VIEW `sasisaidivi.auth_views_synthetic.active_subscription_rate`
AS
SELECT
    COUNTIF(date_end IS NULL) AS active_subscriptions,
    COUNT(*) AS total_subscriptions,
    ROUND(
        SAFE_DIVIDE(
            COUNTIF(date_end IS NULL),
            COUNT(*)
        ) * 100,
        2
    ) AS active_subscription_rate
FROM `sasisaidivi.refine_synthetic.subscriptions`
WHERE is_active = TRUE;