{{
config({
    "post-hook": [
      "{{ index(this, 'localizacao_id')}}",
    ],
    })
}}

with municipios as (

    select * from {{ ref('stg_municipios') }}

),

final as (

    select
        municipios.municipio_id AS localizacao_id,
        municipios.codibge      AS codigo_ibge,
        municipios.nome         AS nome_municipio,
        municipios.uf           AS sigla_estado,
        municipios.uf_nome      AS nome_estado,
        municipios.latitude     AS latitude,
        municipios.longitude    AS longitude

    from municipios

    union 
    
    select
        '00|00000'      AS localizacao_id,
        'Não Definido'  AS codigo_ibge,
        'Não Definido'  AS nome_municipio,
        'ND'            AS sigla_estado,
        'Não Definido'  AS nome_estado,
        0               AS latitude,
        0               AS longitude

)

select * from final