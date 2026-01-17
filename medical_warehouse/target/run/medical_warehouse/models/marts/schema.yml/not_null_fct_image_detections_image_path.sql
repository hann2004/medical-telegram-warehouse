
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select image_path
from "medical_warehouse"."raw"."fct_image_detections"
where image_path is null



  
  
      
    ) dbt_internal_test