{{ config(materialized='table') }}

SELECT
    state_name,
    DATE_TRUNC('month', month)  AS month,
    COUNT(DISTINCT region_id)   AS regions_count,
    AVG(median_rent)            AS avg_rent,
    MIN(median_rent)            AS min_rent,
    MAX(median_rent)            AS max_rent,
    MEDIAN(median_rent)         AS median_state_rent
FROM {{ ref('stg_zori_rent') }}
GROUP BY state_name, DATE_TRUNC('month', month)
