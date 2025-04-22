select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select enrollment_key
from EDU_DB.GOLD.fact_enrollments
where enrollment_key is null



      
    ) dbt_internal_test