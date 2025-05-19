{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_crm_leads on {{ this }} (data, place_id); CLUSTER {{ this }} USING cix_inm_crm_leads;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}


select d.data, place_id, team_id, active, count(*) as submissions
--, sum(case when is_map_crowdfunding_target then 1 else 0 end) as leaders
, count(case when c.is_key_submission then 1 end) as leaders
, count(case when m.value='low' then 1 end) as low_implication
, count(case when m.value='medium' then 1 end) as medium_implication
, count(case when m.value='high' then 1 end) as high_implication
, count(case when m.value='leadership' then 1 end) as leadership_implication
, count(case when ma.value='True' then 1 end) as agregacio_i_flexibilitat_de_la_demanda
, count(case when mc.value='True' then 1 end) as formacio_ciutadana
, count(case when mco.value='True' then 1 end) as compres_collectives
, count(case when mcom.value='True' then 1 end) as generacio_renovable_comunitaria
, count(case when me.value='True' then 1 end) as eficiencia_energetica
, count(case when mr.value='True' then 1 end) as subministrament_energia_100perc_renovable
, count(case when ms.value='True' then 1 end) as mobilitat_sostenible
, count(case when mt.value='True' then 1 end) as energia_terminca_i_climatitzacio
from  {{ source('dwhpublic', 'data')}} d
    join {{ source('dwhexternal', 'hist_odoo_crm_lead')}} c on d.data>=c.dt_start and d.data<c.dt_end
    left join {{ source('dwhexternal', 'hist_odoo_cm_form_submission_metadata')}} m
        on m.key='ce_member_implication'
            and m.submission_id=c.id
            and d.data>=m.dt_start and d.data<m.dt_end
    -- tags
    left join {{ source('dwhexternal', 'hist_odoo_cm_form_submission_metadata')}} ma
        on ma.key='ce_tag_aggregate_demand' and ma.value='True' and ma.submission_id=c.id and d.data>=ma.dt_start and d.data<ma.dt_end
    left join {{ source('dwhexternal', 'hist_odoo_cm_form_submission_metadata')}} mc
        on mc.key='ce_tag_citizen_education' and mc.value='True' and mc.submission_id=c.id and d.data>=mc.dt_start and d.data<mc.dt_end
    left join {{ source('dwhexternal', 'hist_odoo_cm_form_submission_metadata')}} mco
        on mco.key='ce_tag_collective_purchases' and mco.value='True' and mco.submission_id=c.id and d.data>=mco.dt_start and d.data<mco.dt_end
    left join {{ source('dwhexternal', 'hist_odoo_cm_form_submission_metadata')}} mcom
        on mcom.key='ce_tag_common_generation' and mcom.value='True' and mcom.submission_id=c.id and d.data>=mcom.dt_start and d.data<mcom.dt_end
    left join {{ source('dwhexternal', 'hist_odoo_cm_form_submission_metadata')}} me
        on me.key='ce_tag_energy_efficiency' and me.value='True' and me.submission_id=c.id and d.data>=me.dt_start and d.data<me.dt_end
    left join {{ source('dwhexternal', 'hist_odoo_cm_form_submission_metadata')}} mr
        on mr.key='ce_tag_renewable_energy' and mr.value='True' and mr.submission_id=c.id and d.data>=mr.dt_start and d.data<mr.dt_end
    left join {{ source('dwhexternal', 'hist_odoo_cm_form_submission_metadata')}} ms
        on ms.key='ce_tag_sustainable_mobility' and ms.value='True' and ms.submission_id=c.id and d.data>=ms.dt_start and d.data<ms.dt_end
    left join {{ source('dwhexternal', 'hist_odoo_cm_form_submission_metadata')}} mt
        on mt.key='ce_tag_thermal_energy' and mt.value='True' and mt.submission_id=c.id and d.data>=mt.dt_start and d.data<mt.dt_end


where d.data<=current_date
{% if is_incremental() %}
    and d.data>=current_date-5
{% endif %}
group by place_id, d.data, c.team_id, c.active


