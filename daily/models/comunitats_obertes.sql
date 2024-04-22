{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_comunitats_obertes on {{ this }} (data); CLUSTER {{ this }} USING cix_comnitats_obertes;')
) }}


select *
from {{ref('comunitats')}} d
where coalesce(community_status, '')<>'mapa'
    and data<=CURRENT_DATE
    {% if is_incremental() %}
        and data>=current_date-5
    {% endif %}

