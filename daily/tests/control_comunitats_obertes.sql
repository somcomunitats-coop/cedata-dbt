select  
count(ccee.id) as ccee_total, sum(ccee.pack1) as ccee_pack1, sum(ccee.pack2) as ccee_pack2, 
sum(ccee.pmbgsocietaria) as ccee_amb_societaria, sum(ccee.socies) as socies,  
sum(ccee.projectes_auto) as projectes_auto, sum(ccee.kwn) as kwn
from ( 
	select
		rcc.id as coord_id,
		rcc.name as ccord_name,
		rcc.id as ccord_id,
		rc.name as ce_name,
		rc.id as id, 
		case when rc.allow_new_members then 1.0 else 0.0 end as pack2, 
		case when not rc.allow_new_members then 1 else 0 end as pack1,
		case when amb_socies.socies > 0 then 1 else 0 end as pmbgsocietaria,
		amb_socies.socies as socies,
		case when proj_auto.name is not null then 1 else 0 end as projectes_auto,
		proj_auto.power as kwn
	from {{ source('dwhpublic', 'odoo_res_company')}} as rc
	left join odoo_res_company as rcc on rcc.id = rc.parent_id -- coordinadora
	left join ( -- capturem les persones sòcies de cadascuna de les CCEE
		select distinct rrr.id, count(rrr.partner_id) as socies, rrr.name
		from (
			select rc.id, rc.name, rp.id as partner_id
			from {{ source('dwhpublic', 'odoo_res_company_res_partner_rel')}} as rel
			left join {{ source('dwhpublic', 'odoo_res_partner')}} as rp on rp.id = rel.res_partner_id
			left join {{ source('dwhpublic', 'odoo_res_company')}} as rc on rc.id = rel.res_company_id
			where 
			rp.cooperator_register_number is not null and 
			rp.member and rc.hierarchy_level = 'community' and rp.active and rc.name not ilike '%DELETE%' and rc.name not ilike '%Prova%' 
		) as rrr
		group by rrr.id, rrr.name
	) as amb_socies on amb_socies.id = rc.id
	left join ( -- capturem els projectes d'autoconsum en estat de NO esborrany
		select eprj.company_id, eprj.name, esc.power, eprj.state
		from {{ source('dwhpublic', 'odoo_energy_selfconsumption_selfconsumption')}} as esc
		left join {{ source('dwhpublic', 'odoo_energy_project_project')}} as eprj on esc.project_id = eprj.id
		where eprj.state <> 'draft' 
	) as proj_auto on proj_auto.company_id = rc.id 
	where rc.hierarchy_level = 'community' and rc.name not ilike '%DELETE%' and rc.name not ilike '%Prova%'
	order by rc.id
) as ccee
except
select COUNT(distinct id_community) AS cnt,
       count(distinct case when community_pack='Pack 1' then id_community end) AS "Pack1",
       count(distinct case when community_pack='Pack 2' then id_community end) AS "Pack2",
       count(distinct case when te_socies then id_community end) AS "Gestió societària",
       sum(socies) AS "Núm sòcies",
       sum(cnt_autoconsum) AS "Projectes autoconsum",
       sum(pw_autoconsum) AS "Potència autoconsum"
from {{ ref('comunitats_obertes_incr')}}  co
where "data"=current_date
