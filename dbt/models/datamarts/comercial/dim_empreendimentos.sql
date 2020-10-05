{{
config({
    "post-hook": [
      "{{ index(this, 'empreendimento_id')}}",
    ],
    })
}}

with empreendimentos as (

    select * from {{ ref('stg_empreendimentos') }}

),

quadras as (

    select * from {{ ref('stg_quadras') }}

),

final as (

    select
        empreendimentos.cod_pess_empr AS empreendimento_id,
        empreendimentos.codcoligada   AS coligada_id,
        empreendimentos.nomefantasia  AS nome_fantasia,
        empreendimentos.nome          AS nome,
        empreendimentos.sigla         AS sigla,
        COALESCE(empreendimentos.localizacao_id, '00|00000') AS localizacao_id

    from 
        empreendimentos

)

select * from final