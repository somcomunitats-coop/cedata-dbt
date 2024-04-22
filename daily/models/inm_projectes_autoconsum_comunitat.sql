{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_projectes_autoconsum_comunitat on {{ this }} (data, id_community); CLUSTER {{ this }} USING cix_inm_projectes_autoconsum_comunitat;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}


select d.data, eprj.company_id as id_community, sum(esc.power) as pw_autoconsum, count(*) as cnt_autoconsum
from  {{ source('dwhexternal', 'hist_odoo_energy_selfconsumption_selfconsumption')}} as esc
    join {{ source('dwhpublic', 'data')}} d on d.data>=esc.dt_start and d.data<esc.dt_end
    join {{ source('dwhexternal', 'hist_odoo_energy_project_project')}} as eprj on esc.project_id = eprj.id
        and d.data>=eprj.dt_start and d.data<eprj.dt_end
where eprj.state <> 'draft'
    and d.data<=current_date

    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
group by d.data, company_id

