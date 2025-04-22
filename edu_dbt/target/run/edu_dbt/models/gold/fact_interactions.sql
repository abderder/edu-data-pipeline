
  create or replace   view EDU_DB.GOLD.fact_interactions
  
   as (
    

SELECT
    i.interaction_id,
    i.contact_id,
    c.first_name AS contact_first_name,
    c.last_name AS contact_last_name,
    c.email AS contact_email,
    i.interaction_type,
    i.interaction_date,
    i.raw_notes AS notes

FROM EDU_DB.SILVER.silver_interactions i

LEFT JOIN EDU_DB.GOLD.dim_contacts c
    ON i.contact_id = c.contact_id
  );

