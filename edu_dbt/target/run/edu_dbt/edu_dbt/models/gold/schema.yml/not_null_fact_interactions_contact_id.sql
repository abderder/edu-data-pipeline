select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select contact_id
from EDU_DB.GOLD.fact_interactions
where contact_id is null



      
    ) dbt_internal_test