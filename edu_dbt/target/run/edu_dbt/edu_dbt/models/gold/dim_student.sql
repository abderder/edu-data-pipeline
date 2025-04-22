
  create or replace   view EDU_DB.GOLD.dim_student
  
   as (
    

SELECT
    student_id,
    first_name,
    last_name,
    date_of_birth,
    email,
    registration_date
FROM EDU_DB.SILVER.silver_students
  );

