-- Fact table for image detections joined with messages
with detections as (
    select
        image_path,
        detected_class,
        confidence_score,
        image_category
    from "medical_warehouse"."raw"."yolo_detections"
),
msg as (
    select
        m.message_id,
        m.channel_key,
        m.date_key,
        m.image_path as msg_image_path
    from "medical_warehouse"."raw"."fct_messages" m
    where m.image_path is not null
)
select
    msg.message_id,
    msg.channel_key,
    msg.date_key,
    d.detected_class,
    d.confidence_score,
    d.image_category,
    d.image_path
from detections d
left join msg on d.image_path = msg.msg_image_path
where msg.message_id is not null