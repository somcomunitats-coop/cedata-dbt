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
	, coalesce(crs.sr_amount_untaxed,0) as community_sr_amount_untaxed
	, coalesce(crs.sr_amount_untaxed_voluntary,0) as community_sr_amount_untaxed_voluntary
	, coalesce(crs.sr_amount_untaxed_mandatory,0) as community_sr_amount_untaxed_mandatory
    , coalesce(crsco.sr_amount_untaxed,0) as coordinator_sr_amount_untaxed
    , coalesce(crsco.sr_amount_untaxed_voluntary,0) as coordinator_sr_amount_untaxed_voluntary
	, coalesce(crsco.sr_amount_untaxed_mandatory,0) as coordinator_sr_amount_untaxed_mandatory
	, case when s.posted_payment or a.cnt_autoconsum_actiu>0 then true else false end as us_servei_gestio
	, case when s.posted_paid_payment or a.te_quotes_autoconsum then true else false end as us_servei_gestio_ingressos
	, case when a.te_quotes_autoconsum then true else false end as us_servei_gestio_model_autoconsum
	, case when s.posted_paid_payment then true else false end as us_servei_gestio_ingressos_societaris_adicionals
	, case when dt.us_gestio_tributaria or amor.us_amortizacions
	    or eb.us_extractes_bancaris or pp.pagaments_proveidors then true else false end as us_servei_comptabilitat_integral
	, case when a.te_projecte_autoconsum_serv_extern then true else false end as us_servei_monitoritzacio_fotovoltaica
	, a.cups, a.cups_provider
	, coalesce(con.participantes_invitados,0) as participantes_invitados
	, coalesce(s.socies,0)+coalesce(con.participantes_invitados,0) as participantes_totales
	, case when coalesce(s.socies,0)+coalesce(con.participantes_invitados,0)<=100 then 0 else 1 end participantes_totales_gt_100
	, psa.status as pack_serveis_assignat_current_status, psa.pack_servicios
from {{ source('dwhpublic', 'data')}} d
	left join {{ref('inm_community')}} c on d.data=c.data
	left join {{ref('inm_coordinator')}} co on d.data=co.data and co.id_coordinator=c.id_coordinator
	left join {{ref('inm_instance')}} i on d.data=i.data  and co.id_instance=i.id_instance
	left join {{ref('inm_partner')}} pc on co.data=pc.data and co.coordinator_id_partner=pc.id_partner
	left join {{ref('inm_partner')}} p on c.data=p.data and c.community_id_partner=p.id_partner
	left join (
		select cp
            , max(name_ccaa) as ccaa, max(name_provincia) as provincia, max(name_comarca) as comarca
            from  {{ source('dwhpublic', 'geografia_cp')}}  g
            group by cp
	) ubc on p.partner_zip=ubc.cp
	left join {{ref('inm_socies_comunitat')}} s on c.data=s.data and c.id_community=s.id_community
	left join {{ref('inm_projectes_autoconsum_comunitat')}} a on a.data=p.data and c.id_community=a.id_community
	left join {{ref('inm_company_subscription_request')}} crs on d.data=crs.data and c.id_community=crs.company_id
	left join {{ref('inm_company_subscription_request')}} crsco on d.data=crsco.data and co.id_coordinator=crsco.company_id
	left join {{ref('inm_us_gestio_tributaria')}} dt on d.data=dt.data and c.id_community=dt.company_id
	left join {{ref('inm_us_amortitzacions')}} amor on d.data=amor.data and c.id_community=amor.company_id
	left join {{ref('inm_us_extractes_bancaris')}} eb on d.data=eb.data and c.id_community=eb.company_id
	left join {{ref('inm_us_pagaments_proveidors')}} pp on d.data=pp.data and c.id_community=pp.id_community
	left join {{ref('inm_convidades_comunitat')}} con on c.data=con.data and c.id_community=con.id_community
	left join {{ref('inm_pack_serveis_assignat')}} psa on c.data>psa.date_start and c.data<psa.date_end and c.id_community=psa.community_company_id
where d.data<=CURRENT_DATE
    {% if is_incremental() %}
        and d.data>=current_date-5
    {% endif %}

