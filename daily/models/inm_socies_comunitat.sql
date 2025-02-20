{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_socies_comunitat on {{ this }} (data, id_community); CLUSTER {{ this }} USING cix_inm_socies_comunitat;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}

--
--select d.data, rel.res_company_id as id_community, count(*) as socies
--	from  {{ source('dwhexternal', 'hist_odoo_res_company_res_partner_rel')}} rel
--	join {{ source('dwhpublic', 'data')}} d on d.data>=rel.dt_start and d.data<rel.dt_end
--	join {{ source('dwhexternal', 'hist_odoo_res_partner')}} rp on rp.id = rel.res_partner_id
--	    and d.data>=rp.dt_start and d.data<rp.dt_end
--
--where rp.cooperator_register_number is not null
--    and rp.active
--    and rp.member
--    and d.data<=current_date
--
--    {% if is_incremental() %}
--    and d.data>=current_date-5
--    {% endif %}
--
--group by d.data, rel.res_company_id
--

select d.data, company_id as id_community, count(distinct cm.partner_id) as socies
, case when count(case when m.state='posted' then 1 end)>0 then true else false end as posted_payment
, case when count(case when m.state='posted' and m.payment_state='paid' then 1 end)>0 then true else false end as posted_paid_payment
from {{ source('dwhpublic', 'data')}} d
left join {{ source('dwhpublic', 'odoo_cooperative_membership')}} cm on d.data between cm.effective_date and current_date
left join (
    select  m.partner_id, m.dt_start, m.dt_end, m.state, m.payment_state
    from {{ source('dwhexternal', 'hist_odoo_account_move')}} m
        join {{ source('dwhpublic', 'odoo_account_move_line')}} ml on  ml.move_id = m.id
        join {{ source('dwhpublic', 'odoo_account_account')}} a on ml.account_id = a.id
    where a.name='Capital social'
) m on cm.partner_id=m.partner_id and d.data between m.dt_start and m.dt_end
where cm.member is true
    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
group by d.data, company_id
order by 1 desc