
  create or replace   view EDU_DB.GOLD.dim_courses
  
   as (
    

SELECT *,ROW_NUMBER() OVER (ORDER BY COURSE_ID) AS COURSE_KEY
FROM EDU_DB.SILVER.silver_courses
  );

