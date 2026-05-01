{{ config(materialized='table') }}

SELECT
    region_id,
    region_name,
    state_name,
    month,
    median_rent,
    AVG(median_rent) OVER (
        PARTITION BY region_id
        ORDER BY month
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_12m_avg_rent,
    rent_change_mom,
    state_rent_rank
FROM {{ ref('stg_zori_rent') }}
