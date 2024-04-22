
{{ config(materialized='table'
) }}

select u.id, u.municipio as municipi, g.comarca, g.provincia, g.ccaa, u.codpostal
from  {{ source('dwhpublic', 'ubicacio_codipostal')}} u
	left join (
		select "postal code" as cp, max("admin name1") as ccaa, max("admin name2") as provincia, max("Nom comarca")  as comarca
		from  {{ source('dwhpublic', 'tbl_georef')}} tg
		group by "postal code"
	) g on u.codpostal = g.cp