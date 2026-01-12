{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_convidades_comunitat on {{ this }} (data, id_community); CLUSTER {{ this }} USING cix_inm_convidades_comunitat;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}


select d.data, rc.id as id_community, count(rp.id) as participantes_invitados
from {{ source('dwhpublic', 'odoo_ir_property')}} as ip
join {{ source('dwhpublic', 'odoo_ir_model_fields')}} as irmf on irmf.id = ip.fields_id
join {{ source('dwhexternal', 'hist_odoo_res_company')}}  as rc on rc.id = ip.company_id
join {{ source('dwhpublic', 'data')}} d on d.data>=rc.dt_start and d.data<rc.dt_end
join {{ source('dwhexternal', 'hist_odoo_res_partner')}}  as rp on rp.id = substr(ip.res_id,position(',' in ip.res_id)+1)::int
	and d.data>=rp.dt_start and d.data<rp.dt_end
where ip.res_id is not null
and irmf.model = 'res.partner' and irmf.name = 'no_member_autorized_in_energy_actions'
and rp.active
    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
group by rc.id, d.data

