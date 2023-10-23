with 
    cleaned_orders as (
        select 
            orders.order_date 
            , orders.order_date::date as cleaned_date 
            , date_trunc(quarter, orders.order_date::date) as order_quarter
            , date_trunc(month, orders.order_date::date) as order_month 
            , orders.channel_id
            , channels.channel_description
            , orders.customer_type_id
            , customer.customer_type 
            , categories.category
            , categories.sales
        from orders
                cross join 
                lateral (
                    values
                        (orders.fresh, 'Fresh')
                        ,(orders.milk, 'Milk')
                        ,(orders.grocery, 'Grocery')
                        ,(orders.frozen, 'Frozen')
                        ,(orders.detergents_paper, 'Detergents_Paper')
                        ,(orders.delicatessen, 'Delicatessen') 
                ) as categories(sales, category) 
            left join order_channels as channels on orders.channel_id = channels.channel_id
            left join customer_types as customer on orders.customer_type_id = customers.customer_type_id
        )

select *
from cleaned_orders;