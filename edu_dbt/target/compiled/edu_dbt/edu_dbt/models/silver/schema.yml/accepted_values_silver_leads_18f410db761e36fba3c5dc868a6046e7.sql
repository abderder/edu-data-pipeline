
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from EDU_DB.SILVER.silver_leads
    group by status

)

select *
from all_values
where value_field not in (
    'new','qualified','contacted','lost','unknown'
)


