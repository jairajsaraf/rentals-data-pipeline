SELECT *
FROM {{ ref('stg_zori_rent') }}
WHERE month > CURRENT_DATE
