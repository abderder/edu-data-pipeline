
    
    

with all_values as (

    select
        test_column as value_field,
        count(*) as n_records

    from EDU_DB.psps.test_model
    group by test_column

)

select *
from all_values
where value_field not in (
    'hello gold'
)


