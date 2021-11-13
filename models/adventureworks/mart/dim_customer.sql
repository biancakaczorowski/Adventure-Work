{{ config(materialized='table') }}

with
    staging as (
        select 
            customer.customerid
            , customer.personid 
            , customer.storeid
            , customer.territoryid
            --, customer.rowguid
            --, customer.modifieddate
        from {{ ref('stg_customer') }} customer
)
    , transformed as (
        select
            row_number() over (order by customerid) as customer_sk --auto incremental surrogate key, cria uma chave nova baseada em cada id
                , personid 
                , storeid
                , territoryid
                --, rowguid
                --, modifieddate	
        from staging
)

select * from transformed