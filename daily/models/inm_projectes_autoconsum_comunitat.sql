{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_inm_projectes_autoconsum_comunitat on {{ this }} (data, id_community); CLUSTER {{ this }} USING cix_inm_projectes_autoconsum_comunitat;')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}

with pv_quotes as (
    SELECT d.data, ep.company_id, case when count(*)>0 then true else false end as te_projecte_fv_quotes
    FROM {{ source('dwhpublic', 'data')}} d
        join {{ source('dwhexternal', 'hist_odoo_energy_project_project')}} ep on d.data>=ep.dt_start and d.data<ep.dt_end
        JOIN {{ source('dwhexternal', 'hist_odoo_energy_selfconsumption_selfconsumption')}} e ON e.project_id = ep.id and d.data>=e.dt_start and d.data<e.dt_end
        JOIN {{ source('dwhpublic', 'odoo_contract_contract')}} c ON c.project_id = ep.id
        JOIN {{ source('dwhpublic', 'odoo_contract_line')}} cl ON cl.contract_id = c.id
        JOIN {{ source('dwhpublic', 'odoo_account_move_line')}} aml ON aml.contract_line_id = cl.contract_id
        JOIN {{ source('dwhexternal', 'hist_odoo_account_move')}} am ON am.id = aml.move_id  and d.data>=am.dt_start and d.data<am.dt_end
    where ep.state ='active'
    and am.state='posted'
    and am.payment_state ='paid'
    --and d.data='20250101'
    group by d.data, ep.company_id
),
pv_serveis_externs as (
    SELECT d.data, p.company_id, case when count(ess.id)>0 then true else false end as te_projecte_autoconsum_serv_extern
    FROM  {{ source('dwhpublic', 'data')}} d
        join {{ source('dwhexternal', 'hist_odoo_energy_selfconsumption_selfconsumption')}} ess on d.data>=ess.dt_start and d.data<ess.dt_end
        JOIN {{ source('dwhexternal', 'hist_odoo_energy_project_project')}}  p ON p.id = ess.project_id and d.data>=p.dt_start and d.data<p.dt_end
        JOIN {{ source('dwhexternal', 'hist_odoo_energy_project_service_contract')}} esc ON esc.project_id= p.id and d.data>=esc.dt_start and d.data<esc.dt_end
    where esc.active
    group by d.data, p.company_id
),
pv_cups as (
    SELECT d.data, ep.company_id, count(sp.code) as cups
    FROM {{ source('dwhpublic', 'data')}} d
        join {{ source('dwhexternal', 'hist_odoo_energy_project_project')}} ep on d.data>=ep.dt_start and d.data<ep.dt_end
        JOIN {{ source('dwhexternal', 'hist_odoo_energy_selfconsumption_selfconsumption')}} ess ON ess.project_id = ep.id and d.data>=ess.dt_start and d.data<ess.dt_end
        JOIN {{ source('dwhexternal', 'hist_odoo_energy_selfconsumption_distribution_table')}} edt ON edt.selfconsumption_project_id = ess.id  and d.data>=edt.dt_start and d.data<edt.dt_end
        JOIN {{ source('dwhpublic', 'odoo_energy_selfconsumption_supply_point_assignation')}} spa ON spa.distribution_table_id = edt.id and spa.create_date<d.data
        JOIN {{ source('dwhpublic', 'odoo_energy_selfconsumption_supply_point')}} sp ON sp.id = spa.supply_point_id and sp.create_date<d.data
        JOIN {{ source('dwhexternal', 'hist_odoo_energy_project_service_contract')}} sc ON sc.project_id= ep.id and d.data>=sc.dt_start and d.data<sc.dt_end
        JOIN {{ source('dwhpublic', 'odoo_energy_project_provider')}} epro ON sc.provider_id=epro.id
    WHERE  edt.state = 'active'
    group by d.data, ep.company_id
)
select d.data, eprj.company_id as id_community, sum(esc.power) as pw_autoconsum, count(*) as cnt_autoconsum
, count(case when eprj.state='active' then 1 end) as cnt_autoconsum_actiu
, bool_and(coalesce(te_projecte_fv_quotes, false)) as te_quotes_autoconsum
, bool_and(coalesce(te_projecte_autoconsum_serv_extern, false)) as te_projecte_autoconsum_serv_extern
, max(cups) as cups
from  {{ source('dwhexternal', 'hist_odoo_energy_selfconsumption_selfconsumption')}} as esc
    join {{ source('dwhpublic', 'data')}} d on d.data>=esc.dt_start and d.data<esc.dt_end
    join {{ source('dwhexternal', 'hist_odoo_energy_project_project')}} as eprj on esc.project_id = eprj.id
        and d.data>=eprj.dt_start and d.data<eprj.dt_end
    left join pv_quotes q on q.data=d.data and q.company_id=eprj.company_id
    left join pv_serveis_externs se on se.data=d.data and se.company_id=eprj.company_id
    left join pv_cups cu on cu.data=d.data and cu.company_id=eprj.company_id
where eprj.state <> 'draft'
    and d.data<=current_date

    {% if is_incremental() %}
    and d.data>=current_date-5
    {% endif %}
group by d.data, company_id

