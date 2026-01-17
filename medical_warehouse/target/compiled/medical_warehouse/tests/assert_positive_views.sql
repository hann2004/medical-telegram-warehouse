-- Custom test: Ensure view counts are non-negative
select * from "medical_warehouse"."raw"."fct_messages" where view_count < 0