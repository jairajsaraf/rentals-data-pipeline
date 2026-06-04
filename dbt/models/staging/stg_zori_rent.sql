{{ config(materialized='view') }}

SELECT
    CAST(RegionID        AS INTEGER) AS region_id,
    CAST(RegionName      AS VARCHAR) AS region_name,
    CAST(StateName       AS VARCHAR) AS state_name,
    CAST(month           AS DATE)    AS month,
    CAST(median_rent     AS DOUBLE)  AS median_rent,
    CAST(rent_change_mom AS DOUBLE)  AS rent_change_mom,
    CAST(state_rent_rank AS INTEGER) AS state_rent_rank
FROM {{ source('rental_pipeline', 'zori_rent') }}
WHERE StateName IS NOT NULL
