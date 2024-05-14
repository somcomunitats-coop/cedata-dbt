{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_partner on {{ this }} (data, id_partner); CLUSTER {{ this }} USING cix_inm_partner;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}



select p.id as id_partner, p.name as partner_name, p.create_date as partner_create_date, p.parent_id, p.lang as partner_lang, p.active
	, p.zip as partner_zip, coalesce(g.municipi, p.city) as partner_city, p.email as patner_email, p.is_company, p.commercial_partner_id
	, p.cooperator_type, p."member", p.coop_candidate, p.effective_date, p.company_hierarchy_level
	, p.cooperator_register_number
	, g.ccaa, g.provincia, g.comarca
	, d.data
from {{ source('dwhexternal', 'hist_odoo_res_partner')}} p
    left join (
            select cp
            , max(name_ccaa) as ccaa, max(name_provincia) as provincia, max(name_comarca) as comarca
            , max(name_municipi) as municipi
            from  {{ source('dwhpublic', 'geografia_cp')}}  g
            group by cp
        ) g on p.zip=g.cp
    join {{ source('dwhpublic', 'data')}} d on d.data>=p.dt_start and d.data<p.dt_end
where d.data<=current_date

    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
