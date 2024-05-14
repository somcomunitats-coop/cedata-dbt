{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_company_subscription_request on {{ this }} (data, company_id); CLUSTER {{ this }} USING cix_inm_company_subscription_request;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}


select d.data, hsr.company_id, sum(hm.amount_untaxed) as sr_amount_untaxed
    , sum(case when hst.is_voluntary then hm.amount_untaxed) as sr_amount_untaxed_voluntary
    , sum(case when not hst.is_voluntary then hm.amount_untaxed) as sr_amount_untaxed_mandatory
from {{ source('dwhexternal', 'hist_odoo_subscription_request')}} hsr
join {{ source('dwhpublic', 'data')}} d on d.data>=hsr.dt_start and d.data<hsr.dt_end
join {{ source('dwhexternal', 'hist_odoo_account_move')}} hm on  hm.subscription_request = hsr.id and d.data>=hm.dt_start and d.data<hm.dt_end
where hsr.state  in ('done', 'paid')
    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
group by d.data, hsr.company_id

