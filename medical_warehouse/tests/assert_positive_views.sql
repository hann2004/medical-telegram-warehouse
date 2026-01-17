-- Custom test: Ensure view counts are non-negative
select * from {{ ref('fct_messages') }} where view_count < 0