select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select email
from EDU_DB.SILVER.silver_students
where email is null



      
    ) dbt_internal_test