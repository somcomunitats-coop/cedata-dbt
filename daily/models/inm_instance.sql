{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_instance on {{ this }} (data, id_instance); CLUSTER {{ this }} USING cix_inm_instance;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}


select orc.id as id_instance, orc.legal_form as instance_legal_form, orc.name as instance_name , orc.comercial_name  as instance_comercial_name
    , orc.create_date as instance_create_date
    , d.data
from {{ source('dwhexternal', 'hist_odoo_res_company')}} orc
    join {{ source('dwhpublic', 'data')}} d on d.data>=orc.dt_start and d.data<dt_end
where hierarchy_level ='instance'
    and d.data<=current_date

    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
