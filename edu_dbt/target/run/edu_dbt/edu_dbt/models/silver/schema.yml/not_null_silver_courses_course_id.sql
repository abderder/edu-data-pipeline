select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select course_id
from EDU_DB.SILVER.silver_courses
where course_id is null



      
    ) dbt_internal_test