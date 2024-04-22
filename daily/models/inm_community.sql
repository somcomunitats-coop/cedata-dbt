{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_community on {{ this }} (data, id_community); CLUSTER {{ this }} USING cix_inm_community;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}


select orc.id as id_community, orc.legal_form as community_legal_form, orc.name as community_name
	, orc.comercial_name  as community_comercial_name, orc.email as community_email
	, orc.allow_new_members, orc.create_date as community_create_date
	, orc.parent_id as id_coordinator, orc.partner_id as community_id_partner
	, lp.community_type, lp.community_status
	, d.data
from from {{ source('dwhexternal', 'hist_odoo_res_company')}} orc
    join {{ source('dwhpublic', 'data')}} d on d.data>=orc.dt_start and d.data<dt_end
    left join {{ source('dwhexternal', 'hist_odoo_landing_page')}} lp on orc.landing_page_id=lp.id and d.data>=lp.dt_start and d.data<lp.dt_end
where hierarchy_level ='community'
    and d.data<=current_date

    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
