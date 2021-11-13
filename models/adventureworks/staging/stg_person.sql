with source_data as (
    select
        businessentityid
        --, persontype	
        --, namestyle	
        --, title	
        , firstname
        , middlename
        , lastname	
        --, suffix	
        --, emailpromotion	
        --, additionalcontactinfo
        --, demographics	
        --, rowguid	
        --, modifieddate
    from {{ source('adventureworks','person_person') }}
)

select * from source_data