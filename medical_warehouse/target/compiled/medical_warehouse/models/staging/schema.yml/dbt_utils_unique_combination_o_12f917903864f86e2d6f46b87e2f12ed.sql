





with validation_errors as (

    select
        channel_name, message_id
    from "medical_warehouse"."raw"."stg_telegram_messages"
    group by channel_name, message_id
    having count(*) > 1

)

select *
from validation_errors


