CREATE OR REPLACE VIEW
`sasisaidivi.auth_views_synthetic.customer_growth_rate` AS

WITH monthly_customers AS (
    SELECT
        DATE_TRUNC(registration_date, MONTH) AS registration_month,
        COUNT(DISTINCT id) AS customer_count
    FROM `sasisaidivi.refine_synthetic.clients`
    WHERE is_active = TRUE
    GROUP BY registration_month
),

customer_growth AS (
    SELECT
        registration_month,
        customer_count,
        LAG(customer_count) OVER (
            ORDER BY registration_month
        ) AS previous_customer_count
    FROM monthly_customers
)

SELECT
    registration_month,
    customer_count,
    previous_customer_count,
    ROUND(
        SAFE_DIVIDE(
            customer_count - previous_customer_count,
            previous_customer_count
        ) * 100,
        2
    ) AS customer_growth_rate
FROM customer_growth
ORDER BY registration_month;