
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Custom test: Ensure view counts are non-negative
select * from "medical_warehouse"."raw"."fct_messages" where view_count < 0
  
  
      
    ) dbt_internal_test