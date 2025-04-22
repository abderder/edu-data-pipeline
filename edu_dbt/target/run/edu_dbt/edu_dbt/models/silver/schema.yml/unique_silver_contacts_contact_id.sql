select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    contact_id as unique_field,
    count(*) as n_records

from EDU_DB.SILVER.silver_contacts
where contact_id is not null
group by contact_id
having count(*) > 1



      
    ) dbt_internal_test