{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_comunitats_obertes on {{ this }} (data); CLUSTER {{ this }} USING cix_comunitats_obertes;')
) }}


select
    "data", dia_setmana, es_primer_dia_mes, es_ultim_dia_mes, es_primer_dia_trimestre, es_ultim_dia_trimestre
        , es_primer_dia_any, es_ultim_dia_any
    , id_instance, instance_name, instance_create_date
    , id_coordinator, coordinator_name, coordinator_legal_form, coordinator_create_date, coordinator_zip
        , coordinator_city, coordinator_ccaa, coordinator_provincia, coordinator_comarca
        , coordinator_sr_amount_untaxed, coordinator_sr_amount_untaxed_voluntary, coordinator_sr_amount_untaxed_mandatory
    , id_community, community_name, community_email, community_legal_form, community_create_date
        , community_completed_percentage, community_pack, community_type, community_status, community_map_place_id
        , community_zip, community_city, community_ccaa, community_provincia, community_comarca
        , community_sr_amount_untaxed, community_sr_amount_untaxed_voluntary, community_sr_amount_untaxed_mandatory
    , socies, te_socies, pw_autoconsum, cnt_autoconsum, te_autoconsum
    , us_servei_gestio
	, us_servei_gestio_ingressos
	, us_servei_comptabilitat_integral
	, us_servei_monitoritzacio_fotovoltaica
	, cups
from {{ref('comunitats')}} d
where coalesce(community_status, '')<>'mapa'
    and data<=CURRENT_DATE
    {% if is_incremental() %}
        and data>=current_date-5
    {% endif %}

