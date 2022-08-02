{{ config(materialized='table') }}

select date_trunc('month', ts) as any_mes, contract
    , sum(input_active_energy_kwh) as input_active_energy_kwh, sum(output_active_energy_kwh) as output_active_energy_kwh
from ods_curveregistry
group by  date_trunc('month', ts), contract
