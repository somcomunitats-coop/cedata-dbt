{{ config(materialized='incremental', unique_key='comer_contractid') }}

select  coalesce(ct.id,rank() over( order by contract)+(select max(id)from contract)) as id
, o.contract as comer_contractid, o.contract as description
    , coalesce(ct.created_at, current_date) as created_at, coalesce(ct.updated_at, current_Date) as updated_at
    ,case when o.date_end='9999-12-31' then true else false end as is_active, c.id as community_id
from ods_contract_community o
    join community c on o.community =c.name
    left join contract ct on o.contract =ct.comer_contractid and c.id=ct.id
