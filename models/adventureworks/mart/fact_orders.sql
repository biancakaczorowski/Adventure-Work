{{ config(materialized='table') }}

with
    customer as (
        select
            *
        from {{ ref('dim_customer') }}
    )
    , card as (
        select
            *
        from {{ ref('dim_card') }}
    )
    , product as (
        select
            *
        from {{ ref('dim_product') }}
    )
    , reason as (
        select
            *
        from {{ ref('dim_reason') }}
    )
    , region as (
        select
            *
        from {{ ref('dim_region') }}
    )
    , orders_with_sk as (
        select
            reason.salesorder_sk as salesorder_fk
            --, orders.revisionnumber	
            , orders.orderdate	
            --, orders.duedate
            --, orders.shipdate
            , orders.status
            --, orders.onlineorderflag
            --, orders.purchaseordernumber
            --, orders.accountnumber	
            , customer.customer_sk as customer_fk
            --, orders.salespersonid
            --, orders.territoryid	
            , region.address_sk as address_fk	
            --, orders.shiptoaddressid	
            --, orders.shipmethodid	
            , card.creditcard_sk as creditcard_fk	
            --, orders.creditcardapprovalcode	
            , orders.currencyrateid	
            , orders.subtotal	
            , orders.taxamt	
            , orders.freight	
            , orders.totaldue	
            --, orders.comment	
            --, orders.rowguid	
            --, orders.modifieddate
        from {{ ref('stg_order') }} orders 
        left join reason reason on orders.salesorderid = reason.salesorder_sk
        left join customer customer on orders.customerid = customer.customer_sk
        left join region region on orders.billtoaddressid = region.address_sk
        left join card card on orders.creditcardid = card.creditcard_sk
    )
    , order_details_with_sk as (
        select
            order_details.salesorderid as salesorder_fk
            --, order_details.salesorderdetailid	
            --, order_details.carriertrackingnumber	
            , order_details.orderqty
            , product.product_sk as product_fk	
            --, order_details.specialofferid
            , order_details.unitprice	
            , order_details.unitpricediscount
            --, order_details.rowguid
            --, order_details.modifieddate
        from {{ ref('stg_orderdetail') }} order_details
        left join product on order_details.productid = product.product_sk
    )
    , final as (
        select
            order_details_with_sk.salesorder_fk
            --, orders_with_sk.orders.revisionnumber	
            , orders_with_sk.orderdate	
            --, orders_with_sk.duedate
            --, orders_with_sk.shipdate
            , orders_with_sk.status
            --, orders_with_sk.onlineorderflag
            --, orders_with_sk.purchaseordernumber
            --, orders_with_sk.accountnumber	
            , orders_with_sk.customer_fk
            --, orders_with_sk.salespersonid
            --, orders_with_sk.territoryid	
            , orders_with_sk.address_fk	
            --, orders_with_sk.shiptoaddressid	
            --, orders_with_sk.shipmethodid	
            , orders_with_sk.creditcard_fk	
            --, orders_with_sk.creditcardapprovalcode	
            , orders_with_sk.currencyrateid	
            , orders_with_sk.subtotal	
            , orders_with_sk.taxamt	
            , orders_with_sk.freight	
            , orders_with_sk.totaldue	
            --, orders_with_sk.comment	
            --, orders_with_sk.owguid	
            --, orders_with_sk.modifieddate
            --, order_details.salesorderdetailid	
            --, order_details.carriertrackingnumber	
            , order_details_with_sk.orderqty
            , order_details_with_sk.product_fk	
            --, order_details_with_sk.specialofferid
            , order_details_with_sk.unitprice	
            , order_details_with_sk.unitpricediscount
            --, order_details_with_sk.rowguid
            --, order_details_with_sk.modifieddate
        from orders_with_sk
        left join order_details_with_sk on orders_with_sk.salesorder_fk=order_details_with_sk.salesorder_fk
)

select * from final