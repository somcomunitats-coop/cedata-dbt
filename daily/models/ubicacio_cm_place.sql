
{{ config(materialized='table'
) }}

select u.id_cm_place as id
    , coalesce(g.municipi, u.municipi) as municipi
    , g.comarca
    , coalesce(g.provincia, u.provincia) as provincia
    , coalesce(g.ccaa, u.ccaa) as ccaa
    , u.codpostal
from  {{ source('dwhexternal', 'geography_cm_place')}} u
left join (
		select cp
            , max(name_ccaa) as ccaa, max(name_provincia) as provincia, max(name_comarca) as comarca
            , max(name_municipi) as municipi
            from  {{ source('dwhpublic', 'geografia_cp')}}  g
            group by cp
	) g on u.codpostal = g.cp