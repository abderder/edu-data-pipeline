
    
    

select
    enrollment_key as unique_field,
    count(*) as n_records

from EDU_DB.GOLD.fact_enrollments
where enrollment_key is not null
group by enrollment_key
having count(*) > 1


