{{ config(materialized='incremental', unique_key='community') }}

select distinct coalesce(c.id,row_number() over( order by community)+(select max(id)from community)) as id, community as name, community as description
    , coalesce(c.created_at, current_date) as created_at, coalesce(c.updated_at, current_Date) as updated_at, true as is_active
from ods_contract_community o
    left join community c on o.community =c.name
  union
select c.id, c."name" , c.description, c.created_at, c.updated_at, c.is_active
from community c
