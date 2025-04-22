
    
    

select
    course_id as unique_field,
    count(*) as n_records

from EDU_DB.SILVER.silver_courses
where course_id is not null
group by course_id
having count(*) > 1


