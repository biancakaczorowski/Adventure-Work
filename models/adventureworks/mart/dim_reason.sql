{{ config(materialized='table') }}

with
    staging as (
        select
            orderreason.salesorderid
            , orderreason.salesreasonid
            --, orderreason.modifieddate
        from {{ ref('stg_orderreason') }} orderreason
)
    , staging2 as (
        select 
            reason.salesreasonid
            , reason.name
            , reason.reasontype
            --, reason.modifieddate
        from {{ ref('stg_reason') }} reason
)
    , transformed as (
        select
 --           row_number() over (order by salesorderid) as salesorder_sk
                salesorderid
                , staging2.salesreasonid
                , staging2.name as reason_name
                , staging2.reasontype
                --, modifieddate
        from staging
        left join staging2 staging2 on staging.salesreasonid = staging2.salesreasonid
)

select * from transformed