
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from EDU_DB.GOLD.fact_enrollments
    group by status

)

select *
from all_values
where value_field not in (
    'active','inactive','completed','cancelled','unknown'
)


