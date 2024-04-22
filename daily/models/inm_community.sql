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
	, lp.community_type, coalesce(lp.community_status,'') as community_status
	, d.data
from {{ source('dwhexternal', 'hist_odoo_res_company')}} orc
    join {{ source('dwhpublic', 'data')}} d on d.data>=orc.dt_start and d.data<dt_end
    left join {{ source('dwhexternal', 'hist_odoo_landing_page')}} lp on orc.landing_page_id=lp.id and d.data>=lp.dt_start and d.data<lp.dt_end
where hierarchy_level ='community'
    and orc.name not ilike '%DELETE%'
    and orc.name not ilike '%Prova%'
    and d.data<=current_date

    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}

union all
select ocp.id*-1, 'N/A', ocp.name, null, null, true, null, null, null, null, 'mapa' as community_status
, d.data
from data d
	join external.hist_odoo_cm_place ocp on d.data>=ocp.dt_start and d.data<ocp.dt_end
where not exists
    (
        select *
        from data d1
            join "external".hist_odoo_landing_page lp on d1.data>=lp.dt_start and d1.data<lp.dt_end
            join external.hist_odoo_res_company orc on lp.company_id=orc.id and d1.data>=orc.dt_start and d1.data<orc.dt_end
        where lp.map_place_id=ocp.id
            and orc.name not ilike '%DELETE%'
            and orc.name not ilike '%Prova%'
            and orc.hierarchy_level ='community'
	)
	and d.data<=current_date

    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
