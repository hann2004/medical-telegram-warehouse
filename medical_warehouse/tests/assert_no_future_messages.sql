-- Custom test: Ensure no messages have future dates
select f.*
from {{ ref('fct_messages') }} f
left join {{ ref('dim_dates') }} d on f.date_key = d.date_key
where d.full_date > current_date