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

select d.data, cm.company_id as id_community, count(distinct cm.partner_id) as socies
, case when count(case when m.state='posted' then 1 end)>0 then true else false end as posted_payment
, case when count(case when m.state='posted' and m.payment_state='paid' then 1 end)>0 then true else false end as posted_paid_payment
from {{ source('dwhpublic', 'data')}} d
left join {{ source('dwhpublic', 'odoo_cooperative_membership')}} cm on d.data between cm.effective_date and current_date
left join (
    select  m.partner_id, d.data, m.state, m.payment_state, c.id as company_id
    from {{ source('dwhpublic', 'data')}} d
        join {{ source('dwhexternal', 'hist_odoo_account_move')}} m on d.data>=m.dt_Start and d.data<m.dt_end
        join {{ source('dwhpublic', 'odoo_account_move_line')}} ml on ml.move_id = m.id
        join {{ source('dwhpublic', 'odoo_product_product')}} opp on ml.product_id = opp.id
        join {{ source('dwhpublic', 'odoo_product_template')}} as pt on pt.id = opp.product_tmpl_id
        join {{ source('dwhpublic', 'odoo_product_category')}} as pc on pc.id = pt.categ_id
        join {{ source('dwhpublic', 'odoo_ir_model_data')}} d on pc.id=d.res_id
        join {{ source('dwhexternal', 'hist_odoo_res_company')}} c on  c.id = m.company_id and d.data>=c.dt_Start and d.data<c.dt_end
    where m.state='posted'
        and d.name = 'product_category_company_voluntary_share'
) m on cm.partner_id=m.partner_id and d.data = m.data and cm.company_id =m.company_id
where cm.member is true
    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
group by d.data, cm.company_id
order by 1 desc