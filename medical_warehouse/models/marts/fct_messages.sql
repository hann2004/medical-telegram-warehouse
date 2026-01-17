-- Fact table for Telegram messages
with base as (
    select
        message_id,
        channel_name,
        message_date,
        message_text,
        message_length,
        views as view_count,
        forwards as forward_count,
        has_image,
        valid_message,
        image_path
    from {{ ref('stg_telegram_messages') }}
    where valid_message
)
select
    b.message_id,
    c.channel_key,
    d.date_key,
    b.message_text,
    b.message_length,
    b.view_count,
    b.forward_count,
    b.has_image,
    b.image_path
from base b
left join {{ ref('dim_channels') }} c on b.channel_name = c.channel_name
left join {{ ref('dim_dates') }} d on cast(b.message_date as date) = d.full_date