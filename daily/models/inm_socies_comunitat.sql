{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_socies_comunitat on {{ this }} (data, id_community); CLUSTER {{ this }} USING cix_inm_socies_comunitat;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}

select d.data, rel.res_company_id as id_community, count(*) as socies
	from  {{ source('dwhexternal', 'hist_odoo_res_company_res_partner_rel')}} rel
	join {{ source('dwhpublic', 'data')}} d on d.data>=rel.dt_start and d.data<rel.dt_end
	join {{ source('dwhexternal', 'hist_odoo_res_partner')}} rp on rp.id = rel.res_partner_id
	    and d.data>=rp.dt_start and d.data<rp.dt_end

where rp.cooperator_register_number is not null
    and rp.active
    and rp.member
    and d.data<=current_date

    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}

group by d.data, rel.res_company_id

