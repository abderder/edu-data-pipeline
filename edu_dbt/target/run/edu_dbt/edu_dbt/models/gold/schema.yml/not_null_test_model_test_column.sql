select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select test_column
from EDU_DB.psps.test_model
where test_column is null



      
    ) dbt_internal_test