{{ config(materialized='incremental', unique_key='id') }}


select md5(concat(date_trunc('day', ts) ,'-', c.community_id))::UUID as id,
date_trunc('day', ts) as ts, c.community_id as community_id
, sum(input_active_energy_kwh) as input_active_energy_kwh
, sum(output_active_energy_kwh) as output_active_energy_kwh
, current_timestamp as created_at, current_timestamp as updated_at
from ods_curveregistry oc
join contract c on c.comer_contractid =oc.contract and c.is_active=true
where ts>=CURRENT_DATE-5
group by c.community_id,  date_trunc('day', ts)
