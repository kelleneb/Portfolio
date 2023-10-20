/* This is a snippet of code I provided for a techncial exercise while interviewing for a Senior Data Analyst role.
   I was provided with a question about a decrease in sales along with a Mode workbook from a theorhetical junior analyst.
   The deliverables included modified and DRY code, SQL tips to the Junior Analyst, data viz, slide deck, and an email to 
   an executive explaining why sales results appeared down in the most recent quarter. */

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