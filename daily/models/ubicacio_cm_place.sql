
{{ config(materialized='table'
) }}

select u.id::int as id, u.municipio as municipi, g.comarca, g.provincia, g.ccaa, u.codpostal
from  {{ ref('ubicacio_calc')}} u
	left join (
		select cp
            , max(name_ccaa) as ccaa, max(name_provincia) as provincia, max(name_comarca) as comarca
            , max(name_municipi) as municipi
            from  {{ source('dwhpublic', 'geografia_cp')}}  g
            group by cp
	) g on u.codpostal = g.cp