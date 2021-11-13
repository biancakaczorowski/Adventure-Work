with source_data as (
    select
        salesorderid
        , salesreasonid
        --, modifieddate
    from {{ source('adventureworks','sales_salesorderheadersalesreason') }}
)

select * from source_data