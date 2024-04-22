{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_crm_leads on {{ this }} (data, place_id); CLUSTER {{ this }} USING cix_inm_crm_leads;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}


select d.data, place_id, team_id, active, count(*) as submissions
, sum(case when is_map_crowdfunding_target then 1 else 0 end) as leaders
, count(case when m.value='low' then 1 end) as low_implication
, count(case when m.value='medium' then 1 end) as medium_implication
, count(case when m.value='high' then 1 end) as high_implication
, count(case when m.value='leadership' then 1 end) as leadership_implication
from  {{ source('dwhpublic', 'data')}} d
    join {{ source('dwhexternal', 'hist_odoo_crm_lead')}} c on d.data>=c.dt_start and d.data<c.dt_end
    left join {{ source('dwhexternal', 'hist_odoo_cm_form_submission_metadata')}} m
        on m.key='ce_member_implication'
            and m.submission_id=c.id
            and d.data>=m.dt_start and d.data<m.dt_end
where d.data<=current_date
{% if is_incremental() %}
    and d.data>=current_date-5
{% endif %}
group by place_id, d.data, c.team_id, c.active


