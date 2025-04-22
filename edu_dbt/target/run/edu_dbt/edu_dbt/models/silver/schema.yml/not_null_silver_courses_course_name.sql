select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select course_name
from EDU_DB.SILVER.silver_courses
where course_name is null



      
    ) dbt_internal_test