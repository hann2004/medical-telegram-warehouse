
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    channel_key, message_id as unique_field,
    count(*) as n_records

from "medical_warehouse"."raw"."fct_messages"
where channel_key, message_id is not null
group by channel_key, message_id
having count(*) > 1



  
  
      
    ) dbt_internal_test