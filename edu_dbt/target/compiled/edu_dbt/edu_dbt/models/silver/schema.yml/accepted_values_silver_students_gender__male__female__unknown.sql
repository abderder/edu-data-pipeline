
    
    

with all_values as (

    select
        gender as value_field,
        count(*) as n_records

    from EDU_DB.SILVER.silver_students
    group by gender

)

select *
from all_values
where value_field not in (
    'male','female','unknown'
)


