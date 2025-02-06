{{
    config(
        materialized='table'
    )
}}

select * from {{ source('globalmart', 'products') }}