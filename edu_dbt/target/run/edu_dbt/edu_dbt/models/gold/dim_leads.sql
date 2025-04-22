
  create or replace   view EDU_DB.GOLD.dim_leads
  
   as (
    

SELECT *
FROM EDU_DB.SILVER.silver_leads
  );

