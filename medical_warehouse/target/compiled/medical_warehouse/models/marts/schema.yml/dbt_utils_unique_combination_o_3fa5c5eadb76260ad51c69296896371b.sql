





with validation_errors as (

    select
        channel_key, message_id
    from "medical_warehouse"."raw"."fct_messages"
    group by channel_key, message_id
    having count(*) > 1

)

select *
from validation_errors


