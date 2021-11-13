{{ config(materialized='table') }}

with
    staging3 as (
        select
            countryregion.countryregioncode
            , countryregion.name
            --, countryregion.modifieddate	
        from {{ ref('stg_countryregion') }} countryregion
)
    , staging4 as (
        select
            stateprovince.stateprovinceid
            , stateprovince.name
            --, stateprovince.stateprovincecode	
            , stateprovince.countryregioncode
            --, stateprovince.isonlystateprovinceflag
            --, stateprovince.territoryid
            --, stateprovince.rowguid	
            --, stateprovince.modifieddate
        from {{ ref('stg_stateprovince') }} stateprovince
)
    , transformed as (
        select
            row_number() over (order by stateprovinceid) as stateprovince_sk --auto incremental surrogate key, cria uma chave nova baseada em cada id
                , stateprovinceid
                , staging4.name as state_name
                --, stateprovincecode	
                , staging3.countryregioncode
                , staging3.name as country_name
                --, isonlystateprovinceflag
                --, territoryid
                --, rowguid	
                --, modifieddate
        from staging4
        left join staging3 staging3 on staging4.countryregioncode = staging3.countryregioncode
)
    , staging as (
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
    , staging2 as (
        select
            *
        from transformed
)
    , transformed2 as (
        select
            row_number() over (order by addressid) as address_sk --auto incremental surrogate key, cria uma chave nova baseada em cada id
                , addressid
                --, addressline1
                --, addressline2
                , city
                --, postalcode
                --, spatiallocation
                --, rowguid
                --, modifieddate
                , staging2.stateprovince_sk
                , staging2.stateprovinceid
                , staging2.state_name
                , staging2.countryregioncode
                , staging2.country_name
        from staging
        left join staging2 staging2 on staging.stateprovinceid = staging2.stateprovinceid
)
select * from transformed2


