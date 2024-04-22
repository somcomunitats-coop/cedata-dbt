{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_coordinator on {{ this }} (data, id_coordinator); CLUSTER {{ this }} USING cix_inm_coordinator;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}

select orc.id as id_coordinator, orc.legal_form as coordinator_legal_form, orc.name as coordinator_name
, orc.comercial_name  as coordinator_comercial_name, orc.email as coordinator_email
, orc.create_date as coordinator_create_date
, orc.parent_id as id_instance, orc.partner_id as coordinator_id_partner
, d.data
from {{ source('dwhexternal', 'hist_odoo_res_company')}} orc
    join {{ source('dwhpublic', 'data')}} d on d.data>=orc.dt_start and d.data<dt_end
where hierarchy_level ='coordinator'
    and d.data<=current_date

    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
