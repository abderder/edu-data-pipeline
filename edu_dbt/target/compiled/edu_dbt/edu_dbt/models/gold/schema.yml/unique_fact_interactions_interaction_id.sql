
    
    

select
    interaction_id as unique_field,
    count(*) as n_records

from EDU_DB.GOLD.fact_interactions
where interaction_id is not null
group by interaction_id
having count(*) > 1


