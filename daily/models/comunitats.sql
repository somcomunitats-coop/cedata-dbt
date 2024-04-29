{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_comunitats on {{ this }} (data); CLUSTER {{ this }} USING cix_comunitats;')
) }}


select d.data, dia_setmana, d.es_primer_dia_mes, d.es_ultim_dia_mes, d.es_primer_dia_trimestre, d.es_ultim_dia_trimestre, d.es_primer_dia_any, d.es_ultim_dia_any
	, i.id_instance, i.instance_name, i.instance_create_date
	, co.id_coordinator, co.coordinator_name, co.coordinator_legal_form, co.coordinator_create_date
	, pc.partner_zip as coordinator_zip, pc.partner_city as coordinator_city
	, pc.ccaa as coordinator_ccaa, pc.provincia as coordinator_provincia, pc.comarca as coordinator_comarca
	, c.id_community, c.community_name, c.community_email, c.community_legal_form, c.community_create_date
	, c.completed_percentage as community_completed_percentage
	, case when c.allow_new_members then 'Pack 2' when not c.allow_new_members then 'Pack 1' end as community_pack
	, c.community_type, c.community_status
	, c.community_map_place_id
	, p.partner_zip as community_zip, p.partner_city as community_city, p.ccaa as community_ccaa, p.provincia as community_provincia, p.comarca as community_comarca
	, s.socies, case when s.socies is not null then true else false end as te_socies
	, a.pw_autoconsum, a.cnt_autoconsum, case when a.cnt_autoconsum is not null then true else false end as te_autoconsum
from {{ source('dwhpublic', 'data')}} d
	left join {{ref('inm_community')}} c on d.data=c.data
	left join {{ref('inm_coordinator')}} co on d.data=co.data and co.id_coordinator=c.id_coordinator
	left join {{ref('inm_instance')}} i on d.data=i.data  and co.id_instance=i.id_instance
	left join {{ref('inm_partner')}} pc on co.data=pc.data and co.coordinator_id_partner=pc.id_partner
	left join {{ref('inm_partner')}} p on c.data=p.data and c.community_id_partner=p.id_partner
	left join (
		select "postal code" as cp, max("admin name1") as ccaa, max("admin name2") as provincia, max("Nom comarca")  as comarca
		from  {{ source('dwhpublic', 'tbl_georef')}} tg
		group by "postal code"
	) ubc on p.partner_zip=ubc.cp
	left join {{ref('inm_socies_comunitat')}} s on c.data=s.data and c.id_community=s.id_community
	left join {{ref('inm_projectes_autoconsum_comunitat')}} a on a.data=p.data and c.id_community=a.id_community
where d.data<=CURRENT_DATE
    {% if is_incremental() %}
        and d.data>=current_date-5
    {% endif %}

