
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        channel_key, message_id
    from "medical_warehouse"."raw"."fct_messages"
    group by channel_key, message_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test