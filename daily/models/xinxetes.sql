{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_xinxetes on {{ this }} (data); CLUSTER {{ this }} USING cix_xinxetes;')
) }}


select d.data
    , d.dia_setmana, d.es_primer_dia_mes, d.es_ultim_dia_mes
    , d.es_primer_dia_trimestre, d.es_ultim_dia_trimestre
    , d.es_primer_dia_any, d.es_ultim_dia_any
    , cmp.name as map_point_name, max(cmp.completed_percentage) as completed_percentage
    , ub.municipi, ub.comarca, ub.provincia, ub.ccaa, ub.codpostal
    , pc.name as name_place_category
	,count(cmp.id)as xinxetes_sumat
	,sum(case when (cmp.presenter_model_id = 4 and subm.submissions = 0 or subm.submissions is null) then 1 else 0 end) as xinxetes_sumat_sense_persones
	,sum(case when cmp.presenter_model_id = 4 and subm.submissions > 0 then 1 else 0 end) as xinxetes_sumat_amb_persones
	,sum(case when (cmp.presenter_model_id = 4 and cmp.completed_percentage < 50.0 and subm.submissions > 0) then 1 else 0 end ) as xinxetes_sumat_sota_50
	,sum(case when (cmp.presenter_model_id = 4 and cmp.completed_percentage >= 50.0 and cmp.completed_percentage < 100.0 and subm.submissions > 0) then 1 else 0 end ) as xinxetes_sumat_50_100
	,sum(case when cmp.presenter_model_id = 4 then subm.submissions end) as persones
	,sum(case when cmp.presenter_model_id = 4 then subm.leaders end) as persones_leaders
	,sum(case when cmp.presenter_model_id = 4 then subm.low_implication end) as low_implication
	,sum(case when cmp.presenter_model_id = 4 then subm.medium_implication end) as medium_implication
	,sum(case when cmp.presenter_model_id = 4 then subm.high_implication end) as high_implication
	,sum(case when cmp.presenter_model_id = 4 then subm.leadership_implication end) as leadership_implication
	,sum(case when cmp.presenter_model_id = 4 then subm.agregacio_i_flexibilitat_de_la_demanda end) as agregacio_i_flexibilitat_de_la_demanda
    ,sum(case when cmp.presenter_model_id = 4 then subm.formacio_ciutadana end) as formacio_ciutadana
    ,sum(case when cmp.presenter_model_id = 4 then subm.compres_collectives end) as compres_collectives
    ,sum(case when cmp.presenter_model_id = 4 then subm.generacio_renovable_comunitaria end) as generacio_renovable_comunitaria
    ,sum(case when cmp.presenter_model_id = 4 then subm.eficiencia_energetica end) as eficiencia_energetica
    ,sum(case when cmp.presenter_model_id = 4 then subm.subministrament_energia_100perc_renovable end) as subministrament_energia_100perc_renovable
    ,sum(case when cmp.presenter_model_id = 4 then subm.mobilitat_sostenible end) as mobilitat_sostenible
    ,sum(case when cmp.presenter_model_id = 4 then subm.energia_terminca_i_climatitzacio end) as energia_terminca_i_climatitzacio
    ,count(case when cmp.presenter_model_id = 4 and cmp.key_submissions_target_reached then 1 end) as grup_motor_potencial
    ,count(case when cmp.presenter_model_id = 4 and cmp.key_group_activated then 1 end) as grup_motor_activat
    ,sum(case when cmp.presenter_model_id = 4 and cmp.key_submissions_target_reached then subm.leaders else 0 end) as persones_leaders_cmp
    ,sum(case when cmp.presenter_model_id = 4 and cmp.key_group_activated then subm.leaders else 0 end) as persones_leaders_cma
    ,sum(case when cmp.presenter_model_id = 4 and cmp.key_submissions_target_reached then subm.submissions else 0 end) as persones_cmp
    ,sum(case when cmp.presenter_model_id = 4 and cmp.key_group_activated then subm.submissions else 0 end) as persones_cma
    ,sum(case when (cmp.presenter_model_id = 4 and cmp.completed_percentage >= 100.0 and subm.submissions > 0) then 1 else 0 end ) as xinxetes_sumat_100_o_mes
    ,sum(case when cmp.presenter_model_id = 4 and subm.leaders>0 then 1 end ) as xinxetes_amb_leaders
    ,sum(case when cmp.presenter_model_id = 4 and subm.leaders>0 then submissions end ) as persones_en_xinxetes_amb_leaders
    ,count(case when cmp.presenter_model_id=7 then 1 end) as ccee_externas
    , m.name as mapa, rp.name as company
from {{ source('dwhpublic', 'data')}} d
left join {{ source('dwhexternal', 'hist_odoo_cm_place')}} cmp on d.data>=cmp.dt_start and d.data<cmp.dt_end
left join {{ source('dwhexternal', 'hist_odoo_cm_place_category')}} pc on cmp.place_category_id=pc.id and d.data>=pc.dt_start and d.data<pc.dt_end
left join {{ ref('ubicacio_cm_place')}} ub on cmp.id=ub.id
left join {{ source('dwhpublic', 'odoo_cm_map')}} m on cmp.company_id = m.company_id
left join {{ source('dwhpublic', 'odoo_res_company')}} rp on m.company_id = rp.id
left join (
	select data, place_id
	    , sum(submissions) as submissions
	    , sum(leaders) as leaders
	    , sum(low_implication) as low_implication
	    , sum(medium_implication) as medium_implication
        , sum(high_implication) as high_implication
        , sum(leadership_implication) as leadership_implication
        , sum(agregacio_i_flexibilitat_de_la_demanda) as agregacio_i_flexibilitat_de_la_demanda
        , sum(formacio_ciutadana) as formacio_ciutadana
        , sum(compres_collectives) as compres_collectives
        , sum(generacio_renovable_comunitaria) as generacio_renovable_comunitaria
        , sum(eficiencia_energetica) as eficiencia_energetica
        , sum(subministrament_energia_100perc_renovable) as subministrament_energia_100perc_renovable
        , sum(mobilitat_sostenible) as mobilitat_sostenible
        , sum(energia_terminca_i_climatitzacio) as energia_terminca_i_climatitzacio
	from {{ ref('inm_crm_leads')}}
	where active
	    --and team_id = 5 -- map Sumbmissions
	group by place_id, data
	) subm on subm.place_id = cmp.id and subm.data=d.data
where 1=1
    --and cmp.presenter_model_id = 4  -- per activar 20251002, passat a les mètriques
    --and cmp.company_id = 1 -- instancia
    and cmp.status = 'published'
    and cmp.active
    and d.data<=current_date

    {% if is_incremental() %}
        and d.data>=current_date-5
    {% endif %}

group by d.data
    , d.dia_setmana, d.es_primer_dia_mes, d.es_ultim_dia_mes
    , d.es_primer_dia_trimestre, d.es_ultim_dia_trimestre
    , d.es_primer_dia_any, d.es_ultim_dia_any
    , cmp.name
    , ub.municipi, ub.comarca, ub.provincia, ub.ccaa, ub.codpostal
    , pc.name
    , m.name, rp.name