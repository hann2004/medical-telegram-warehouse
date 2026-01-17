-- Dimension table for Telegram channels
with base as (
    select
        channel_name,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts,
        avg(views) as avg_views
    from "medical_warehouse"."raw"."stg_telegram_messages"
    where valid_message
    group by channel_name
)
select
    row_number() over (order by channel_name) as channel_key,
    channel_name,
    'Unknown' as channel_type, -- You can update this with real types if available
    first_post_date,
    last_post_date,
    total_posts,
    avg_views
from base