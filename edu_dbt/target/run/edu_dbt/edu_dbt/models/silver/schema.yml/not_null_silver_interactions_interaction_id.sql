select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select interaction_id
from EDU_DB.SILVER.silver_interactions
where interaction_id is null



      
    ) dbt_internal_test