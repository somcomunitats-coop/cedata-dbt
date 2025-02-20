{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_us_gestio_tributaria on {{ this }} (data, id_community); CLUSTER {{ this }} USING cix_inm_us_gestio_tributaria;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}


--



select d."data", v.company_id, case when count(*)>0 then true else false end as us_gestio_tributaria
from {{ source('dwhpublic', 'data')}} d
left join (
    select *
    from (
        select state, create_Date, company_id
        from  {{ source('dwhpublic', 'odoo_l10n_es_aeat_mod123_report')}}
        union all
        select state, create_Date, company_id
        from  {{ source('dwhpublic', 'odoo_l10n_es_aeat_mod111_report')}}
        union all
        select state, create_Date, company_id
        from  {{ source('dwhpublic', 'odoo_l10n_es_aeat_mod347_report')}}
        union all
        select state, create_Date, company_id
        from  {{ source('dwhpublic', 'odoo_l10n_es_aeat_mod303_report')}}
        union all
        select state, create_Date, company_id
        from  {{ source('dwhpublic', 'odoo_l10n_es_aeat_mod390_report')}}
        union all
        select state, create_Date, company_id
        from  {{ source('dwhpublic', 'odoo_l10n_es_vat_book')}}
        ) a
    where a.state in ('calculate', 'done', 'posted')
    ) v on v.create_date<=d."data"
where 1=1
    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
group by d.data, v.company_id


