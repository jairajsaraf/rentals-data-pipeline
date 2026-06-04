SELECT region_id, month, COUNT(*) AS n
FROM {{ ref('stg_zori_rent') }}
GROUP BY region_id, month
HAVING COUNT(*) > 1
