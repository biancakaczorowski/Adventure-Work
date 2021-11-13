with source_data as (
    select
        countryregioncode
        --, name
        --, modifieddate
    from {{ source('adventureworks','person_countryregion') }}
)

select * from source_data