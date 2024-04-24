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
	,sum(case when (subm.submissions = 0 or subm.submissions is null) then 1 else 0 end) as xinxetes_sumat_sense_persones
	,sum(case when subm.submissions > 0 then 1 else 0 end) as xinxetes_sumat_amb_persones
	,sum(case when (cmp.completed_percentage < 50.0 and subm.submissions > 0) then 1 else 0 end ) as xinxetes_sumat_sota_50
	,sum(case when (cmp.completed_percentage >= 50.0 and cmp.completed_percentage < 100.0 and subm.submissions > 0) then 1 else 0 end ) as xinxetes_sumat_50_100
	,sum(subm.submissions) as persones
	,sum(subm.leaders) as persones_leaders
	,sum(subm.low_implication) as low_implication
	,sum(subm.medium_implication) as medium_implication
	,sum(subm.high_implication) as high_implication
	,sum(subm.leadership_implication) as leadership_implication
from {{ source('dwhpublic', 'data')}} d
left join {{ source('dwhexternal', 'hist_odoo_cm_place')}} cmp on d.data>=cmp.dt_start and d.data<cmp.dt_end
left join {{ source('dwhexternal', 'hist_odoo_cm_place_category')}} pc on cmp.place_category_id=pc.id and d.data>=pc.dt_start and d.data<pc.dt_end
left join {{ ref('ubicacio_cm_place')}} ub on cmp.id=ub.id
left join (
	select data, place_id
	    , sum(submissions) as submissions
	    , sum(leaders) as leaders
	    , sum(low_implication) as low_implication
	    , sum(medium_implication) as medium_implication
        , sum(high_implication) as high_implication
        , sum(leadership_implication) as leadership_implication
	from {{ ref('inm_crm_leads')}}
	where active
	    and team_id = 5 -- map Sumbmissions
	group by place_id, data
	) subm on subm.place_id = cmp.id and subm.data=d.data
where
    cmp.presenter_model_id = 4  -- per activar
    and cmp.company_id = 1 -- instancia
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
