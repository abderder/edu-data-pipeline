
    
    

with all_values as (

    select
        source as value_field,
        count(*) as n_records

    from EDU_DB.SILVER.silver_leads
    group by source

)

select *
from all_values
where value_field not in (
    'web','email','social','referral','unknown'
)


