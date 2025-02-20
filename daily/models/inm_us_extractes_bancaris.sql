{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_us_extractes_bancaris on {{ this }} (data, id_community); CLUSTER {{ this }} USING cix_inm_us_extractes_bancaris;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}


select d."data", v.company_id, case when count(*)>0 then true else false end as us_extractes_bancaris
from {{ source('dwhpublic', 'data')}} d
left join (
    select company_id, state, date
    from {{ source('dwhpublic', 'odoo_account_bank_statement')}}
    where state in ('posted', 'confirm')
    ) v on v.create_date<=d."data"
where 1=1
    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
group by d.data, v.company_id


