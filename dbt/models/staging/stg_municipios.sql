
with municipios as (

    select * from staging.municipios

),

estados as (

    select * from staging.estados

),

gmunicipio as (

    select * from staging.gmunicipio

),

final as (

    select 
        concat(gmunicipio.codetdmunicipio, '|', gmunicipio.codmunicipio) AS municipio_id,
        municipios.codibge::varchar(7),
        municipios.nome,
        estados.uf,
        estados.nome as uf_nome,
        municipios.latitude,
        municipios.longitude

    from municipios

    join estados on municipios.codigo_uf = estados.codigo_uf 

    join gmunicipio on municipios.codibge::varchar(7) = gmunicipio.codibge

)

select * from final