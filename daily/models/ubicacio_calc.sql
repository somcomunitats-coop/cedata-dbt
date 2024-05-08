
{{ config(materialized='view'
) }}

with ubicacions as (
	select uc.*, o.address_txt
	from {{ source('dwhpublic', 'ubicacio_codipostal')}} uc
	join {{ source('dwhpublic', 'odoo_cm_place')}} o on uc.id=o.id::varchar
)
,newubicacions as (
select u.id, min(u2.id) as new_id
from ubicacions u
	join ubicacions u2 on u.address_txt=u2.address_txt and u2.observaciones_geocodificacion='Se ha encontrado dirección'
where u.observaciones_geocodificacion ='Dirección no encontrada'
group by u.id
)
select *
from ubicacio_codipostal
where observaciones_geocodificacion='Se ha encontrado dirección'
union all
select n.id, u.tipo_via, u.nombre_via, u.portal1, u.portal2, u.codpostal, u.cod_ine_municipio, u.latitud_wgs84_4326, u.longitud_wgs84_4326, u.municipio, u.provincia
, 'Dirección calculada'
from newubicacions n
	join ubicacions u on n.new_id=u.id