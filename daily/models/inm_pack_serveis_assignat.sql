{{ config(materialized='table'
 , post_hook=after_commit('create index IF NOT EXISTS cix_{{ this.table }} on {{ this }} (data, company_id); CLUSTER {{ this }} USING cix_{{ this.table }};')
 , schema='inm'
 , docs={'node_color': '#b3b325'}
) }}

select cc.community_company_id, cc.status, pt.default_code as pack_servicios
	, cc.date_start, coalesce(cc.date_end, '9999-12-31') as date_end
from {{ source('dwhpublic', 'odoo_contract_contract')}} as cc
join {{ source('dwhpublic', 'odoo_res_company')}} as rc on rc.id = cc.company_id
join {{ source('dwhpublic', 'odoo_sale_order_line')}} sol on sol.id = cc.sale_order_id and product_id is not null
left join {{ source('dwhpublic', 'odoo_product_product')}} as pp on pp.id = sol.product_id
left join {{ source('dwhpublic', 'odoo_product_template')}} as pt on pt.id = pp.product_tmpl_id
where
    cc.community_company_id is not null
    and rc.hierarchy_level ='instance' -- els contractes de packs de serveis de tarifes sempre estan a nivell de la companyia de la instancia
    and cc.pack_type = 'platform_pack'
    and cc.successor_contract_id is null
