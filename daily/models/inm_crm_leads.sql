{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_crm_leads on {{ this }} (data, place_id); CLUSTER {{ this }} USING cix_inm_crm_leads;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}


select d.data, place_id, team_id, active, count(id) as submissions, sum(case when is_map_crowdfunding_target then 1 else 0 end) as leaders
from  {{ source('dwhpublic', 'data')} d
    join {{ source('dwhexternal', 'hist_odoo_crm_lead')}} c on d.data>=dt_start and d.data<dt_end
where d.data<=current_date
{% if is_incremental() %}
    and d.data>=current_date-5
{% endif %}
group by place_id, d.data, team_id, active


