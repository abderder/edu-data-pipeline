
  create or replace   view EDU_DB.GOLD.dim_contacts
  
   as (
    

SELECT *
FROM EDU_DB.SILVER.silver_contacts
  );

