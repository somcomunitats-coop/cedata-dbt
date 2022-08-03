{{ config(materialized='table') }}

select
date_trunc('month', ts) as ts, c.community_id as community_id
, sum(input_active_energy_kwh) as input_active_energy_kwh
, sum(output_active_energy_kwh) as output_active_energy_kwh
, current_timestamp as created_at, current_timestamp as updated_at
from ods_curveregistry oc
join contract c on c.comer_contractid =oc.contract
group by c.community_id,  date_trunc('month', ts)
