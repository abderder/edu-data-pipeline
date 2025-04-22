select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

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



      
    ) dbt_internal_test