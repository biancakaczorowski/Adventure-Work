{{ config(materialized='table') }}

with
    staging as (
        select 
            address.addressid
            --, address.addressline1
            --, address.addressline2
            , address.city
            , address.stateprovinceid
            --, address.postalcode
            --, address.spatiallocation
            --, address.rowguid
            --, address.modifieddate
        from {{ ref('stg_address') }} address
)
--    , staging2 as (
--        select
--            stateprovince.stateprovinceid
            --, stateprovince.stateprovincecode	
--            , stateprovince.countryregioncode
            --, stateprovince.isonlystateprovinceflag
            --, stateprovince.name	
            --, stateprovince.territoryid
            --, stateprovince.rowguid	
            --, stateprovince.modifieddate
--        from {{ ref('stg_stateprovince') }} stateprovince
--)
    , staging3 as (
        select
            countryregion.countryregioncode
            --, countryregion.name
            --, countryregion.modifieddate	
        from {{ ref('stg_countryregion') }} countryregion
)
    , staging4 as (
        select
            stateprovince.stateprovinceid
            --, stateprovince.stateprovincecode	
            , stateprovince.countryregioncode
            --, stateprovince.isonlystateprovinceflag
            --, stateprovince.name	
            --, stateprovince.territoryid
            --, stateprovince.rowguid	
            --, stateprovince.modifieddate
        from {{ ref('stg_stateprovince') }} stateprovince
        left join staging3 staging3 on stateprovince.countryregioncode = staging3.countryregioncode
)
    , transformed as (
        select
            row_number() over (order by addressid) as address_sk --auto incremental surrogate key, cria uma chave nova baseada em cada id
                --, addressline1
                --, addressline2
                , city
                --, postalcode
                --, spatiallocation
                --, rowguid
                --, modifieddate
                ,staging4.stateprovinceid
        from staging
        left join staging4 staging4 on staging.stateprovinceid = staging4.stateprovinceid
)

select * from transformed