with source_data as (
    select
        customerid
        , personid 
        , storeid
        , territoryid
        --, rowguid
        --, modifieddate
    from {{ source('adventureworks','sales_customer') }}
)

select * from source_data