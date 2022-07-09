{{ config(materialized='table') }}

SELECT
    stg_yellow_tripadta.vendorid
FROM    
    {{ ref('stg_yellow_tripdata') }}