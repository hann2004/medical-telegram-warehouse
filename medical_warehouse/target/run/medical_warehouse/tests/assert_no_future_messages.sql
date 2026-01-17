
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Custom test: Ensure no messages have future dates
select f.*
from "medical_warehouse"."raw"."fct_messages" f
left join "medical_warehouse"."raw"."dim_dates" d on f.date_key = d.date_key
where d.full_date > current_date
  
  
      
    ) dbt_internal_test