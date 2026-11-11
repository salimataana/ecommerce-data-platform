SELECT
    category,
    total_products,
    average_price,
    total_stock,

    CASE
        WHEN total_stock < 300 THEN 'Stock faible'
        ELSE 'Stock suffisant'
    END AS stock_status

FROM {{ source('ecommerce', 'products_by_category') }}