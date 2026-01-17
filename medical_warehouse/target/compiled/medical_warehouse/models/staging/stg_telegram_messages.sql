-- Staging model for telegram_messages
with raw as (
    select
        message_id,
        channel_name,
        cast(message_date as timestamp) as message_date,
        message_text,
        cast(has_media as boolean) as has_media,
        image_path,
        cast(views as integer) as views,
        cast(forwards as integer) as forwards,
        cast(scraped_at as timestamp) as scraped_at,
        cast(message_length as integer) as message_length,
        cast(contains_price as boolean) as contains_price,
        cast(contains_contact as boolean) as contains_contact,
        row_number() over (partition by channel_name, message_id order by scraped_at desc) as rn
    from "medical_warehouse"."raw"."telegram_messages"
)
select
    *,
    message_text is not null and length(message_text) > 0 as valid_message,
    has_media as has_image
from raw
where rn = 1 and message_id is not null