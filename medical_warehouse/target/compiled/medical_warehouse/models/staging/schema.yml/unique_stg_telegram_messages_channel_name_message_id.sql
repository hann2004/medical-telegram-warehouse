
    
    

select
    channel_name, message_id as unique_field,
    count(*) as n_records

from "medical_warehouse"."raw"."stg_telegram_messages"
where channel_name, message_id is not null
group by channel_name, message_id
having count(*) > 1


