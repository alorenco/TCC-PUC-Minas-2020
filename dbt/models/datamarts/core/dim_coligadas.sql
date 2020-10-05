{{
config({
    "post-hook": [
      "{{ index(this, 'coligada_id')}}",
    ],
    })
}}

with coligadas as (

    select * from {{ ref('stg_coligadas') }}

),
final as (

    select
        coligadas.codcoligada  AS coligada_id,
        coligadas.nome         AS nome_coligada,
        coligadas.nomefantasia AS nome_fantasia,
        coligadas.cidade       AS cidade,
        coligadas.estado       AS estado,
        coligadas.cep          AS cep

    from coligadas

)

select * from final