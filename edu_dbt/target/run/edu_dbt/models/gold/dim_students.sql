
  create or replace   view EDU_DB.GOLD.dim_students
  
   as (
    

SELECT *, ROW_NUMBER() OVER (ORDER BY STUDENT_ID) AS student_key

FROM EDU_DB.SILVER.silver_students
  );

