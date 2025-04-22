select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        interaction_type as value_field,
        count(*) as n_records

    from EDU_DB.SILVER.silver_interactions
    group by interaction_type

)

select *
from all_values
where value_field not in (
    'call','email','meeting','appointment','unknown'
)



      
    ) dbt_internal_test