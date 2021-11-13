with source_data as (
    select
        stateprovinceid
        --, stateprovincecode	
        , countryregioncode
        --, isonlystateprovinceflag
        --, name	
        --, territoryid
        --, rowguid	
        --, modifieddate
    from {{ source('adventureworks','person_stateprovince') }}
)

select * from source_data