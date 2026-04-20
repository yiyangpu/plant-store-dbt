with orders as (
    select *
    from {{ ref('base_web_orders') }}
),

returns as (
    select *
    from {{ ref('base_google_drive_returns') }}
),

dedup_orders as (
    select *
    from (
        select *,
               row_number() over (
                   partition by order_id
                   order by order_time desc
               ) as rn
        from orders
    )
    where rn = 1
),

joined as (
    select
        o.order_id,
        o.order_time,
        o.price_per_unit,
        o.add_to_cart_quantity,
        r.is_refunded
    from dedup_orders o
    left join returns r
        on o.order_id = r.order_id
),

final as (
    select
        order_id,
        order_time,
        is_refunded,

        case
            when is_refunded = true then 0
            else price_per_unit * add_to_cart_quantity
        end as revenue
    from joined
)

select * from final