{{ config(materialized='incremental'
 , unique_key='data'
 , post_hook=after_commit('create index IF NOT EXISTS cix_comunitats_obertes_incr on {{ this }} (data); CLUSTER {{ this }} USING cix_comunitats_obertes_incr;')
) }}


select co.*
	, case when ca.id_community is null then 1 when co.id_community is null then -1 else 0 end as increment_comunitat
	, coalesce(co.coordinator_sr_amount_untaxed,0)-coalesce(ca.coordinator_sr_amount_untaxed,0) as increment_coordinator_sr_amount_untaxed
	, coalesce(co.coordinator_sr_amount_untaxed_voluntary,0)-coalesce(ca.coordinator_sr_amount_untaxed_voluntary,0) as increment_coordinator_sr_amount_untaxed_voluntary
	, coalesce(co.coordinator_sr_amount_untaxed_mandatory,0)-coalesce(ca.coordinator_sr_amount_untaxed_mandatory,0) as increment_coordinator_sr_amount_untaxed_mandatory
	, coalesce(co.community_completed_percentage,0)-coalesce(ca.community_completed_percentage,0) as increment_community_completed_percentage
	, coalesce(co.community_sr_amount_untaxed,0)-coalesce(ca.community_sr_amount_untaxed,0) as increment_community_sr_amount_untaxed
	, coalesce(co.community_sr_amount_untaxed_voluntary,0)-coalesce(ca.community_sr_amount_untaxed_voluntary,0) as increment_community_sr_amount_untaxed_voluntary
	, coalesce(co.community_sr_amount_untaxed_mandatory,0)-coalesce(ca.community_sr_amount_untaxed_mandatory,0) as increment_community_sr_amount_untaxed_mandatory
	, coalesce(co.socies,0)-coalesce(ca.socies,0) as increment_socies
	, coalesce(co.cups,0)-coalesce(ca.cups,0) as increment_cups
from {{ref('comunitats_obertes')}} co
	full join {{ref('comunitats_obertes')}} ca on co.data=ca.data+1 and co.id_community=ca.id_community
where co.data<=CURRENT_DATE
    {% if is_incremental() %}
        and co.data>=current_date-5
    {% endif %}

