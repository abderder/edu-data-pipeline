select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select lead_id
from EDU_DB.SILVER.silver_leads
where lead_id is null



      
    ) dbt_internal_test