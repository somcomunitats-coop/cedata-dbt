{{ config(materialized='table') }}

select md5(concat(date_trunc('week', ts) ,'-', c.id))::UUID as id,
date_trunc('week', ts) as ts, c.id as contract_id
, sum(input_active_energy_kwh) as input_active_energy_kwh
, sum(output_active_energy_kwh) as output_active_energy_kwh
, current_timestamp as created_at, current_timestamp as updated_at
from ods_curveregistry oc
join contract c on c.comer_contractid =oc.contract
group by c.id,  date_trunc('week', ts)
