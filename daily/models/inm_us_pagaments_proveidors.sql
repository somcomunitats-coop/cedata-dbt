{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_us_pagaments_proveidors on {{ this }} (data, id_community); CLUSTER {{ this }} USING cix_inm_us_pagaments_proveidors;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}


select d.data, m.company_id as id_community
, case when count(*)>0 then true else false end as pagaments_proveidors
from {{ source('dwhpublic', 'data')}} d
left join (
    select  m.company_id, m.dt_start, m.dt_end, m.state, m.payment_state
    from {{ source('dwhexternal', 'hist_odoo_account_move')}} m
       join {{ source('dwhpublic', 'odoo_account_journal')}} j on m.journal_id=j.id
    where j.code='BILL'
        and m.state='posted'
    ) m on d.data between m.dt_start and m.dt_end
where 1=1
    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
group by d.data, m.company_id
order by 1 desc