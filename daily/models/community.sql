{{ config(materialized='incremental', unique_key='community') }}

select distinct coalesce(c.id,rank() over( order by community)+(select max(id)from community)) as id, community as name, community as description
    , coalesce(c.created_at, current_date) as created_at, coalesce(c.updated_at, current_Date) as updated_at, true as is_active
from ods_contract_community o
    left join community c on o.community =c.name
