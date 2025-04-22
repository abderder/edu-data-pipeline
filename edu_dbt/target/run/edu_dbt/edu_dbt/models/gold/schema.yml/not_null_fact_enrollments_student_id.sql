select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select student_id
from EDU_DB.GOLD.fact_enrollments
where student_id is null



      
    ) dbt_internal_test