MERGE `sasisaidivi.refine_synthetic.subscriptions` AS tar
USING (
    -- Close the existing active version
    SELECT
        s.id,
        s.client_id,
        s.product_category,
        s.product_company,
        s.amount,
        s.date_start,
        s.date_end,
        'update' AS type_op
    FROM `sasisaidivi.raw_synthetic.subscriptions_transfer` AS s
    JOIN `sasisaidivi.refine_synthetic.subscriptions` AS t
        ON s.id = t.id
       AND t.is_active = TRUE
    WHERE
        s.date_end IS DISTINCT FROM t.date_end
        OR s.date_start IS DISTINCT FROM t.date_start
        OR s.amount IS DISTINCT FROM t.amount
        OR s.product_company IS DISTINCT FROM t.product_company
        OR s.product_category IS DISTINCT FROM t.product_category

    UNION ALL

    -- Insert the new version for changed subscriptions
    SELECT
        s.id,
        s.client_id,
        s.product_category,
        s.product_company,
        s.amount,
        s.date_start,
        s.date_end,
        'insert' AS type_op
    FROM `sasisaidivi.raw_synthetic.subscriptions_transfer` AS s
    JOIN `sasisaidivi.refine_synthetic.subscriptions` AS t
        ON s.id = t.id
       AND t.is_active = TRUE
    WHERE
        s.date_end IS DISTINCT FROM t.date_end
        OR s.date_start IS DISTINCT FROM t.date_start
        OR s.amount IS DISTINCT FROM t.amount
        OR s.product_company IS DISTINCT FROM t.product_company
        OR s.product_category IS DISTINCT FROM t.product_category

    UNION ALL

    -- Insert completely new subscriptions
    SELECT
        s.id,
        s.client_id,
        s.product_category,
        s.product_company,
        s.amount,
        s.date_start,
        s.date_end,
        'insert' AS type_op
    FROM `sasisaidivi.raw_synthetic.subscriptions_transfer` AS s
    LEFT JOIN `sasisaidivi.refine_synthetic.subscriptions` AS t
        ON s.id = t.id
    WHERE t.id IS NULL
) AS sou

ON tar.id = sou.id
AND tar.is_active = TRUE
AND sou.type_op = 'update'

WHEN MATCHED THEN
    UPDATE SET
        tar.is_active = FALSE,
        tar.active_timestamp = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        id,
        client_id,
        product_category,
        product_company,
        amount,
        date_start,
        date_end,
        is_active,
        active_timestamp
    )
    VALUES (
        sou.id,
        sou.client_id,
        sou.product_category,
        sou.product_company,
        sou.amount,
        sou.date_start,
        sou.date_end,
        TRUE,
        CURRENT_TIMESTAMP()
    );