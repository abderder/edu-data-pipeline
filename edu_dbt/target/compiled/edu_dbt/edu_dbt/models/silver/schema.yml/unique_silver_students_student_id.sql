
    
    

select
    student_id as unique_field,
    count(*) as n_records

from EDU_DB.SILVER.silver_students
where student_id is not null
group by student_id
having count(*) > 1


