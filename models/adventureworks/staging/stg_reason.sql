with source_data as (
    select
        salesreasonid
        --, name
        , reasontype
        --, modifieddate
    from {{ source('adventureworks','sales_salesreason') }}
)

select * from source_data