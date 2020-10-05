{{
config({
    "post-hook": [
      "{{ index(this, 'gerente_id')}}",
    ],
    })
}}

with gerentes as (

    select * from {{ ref('stg_gerentes') }}

),

final as (

    select
        gerentes.codcfo            AS gerente_id,
        gerentes.nome              AS nome,
        gerentes.nomefantasia      AS nome_fantasia,
        gerentes.cidade            AS cidade,
        gerentes.codetd            AS estado

    from 
        gerentes

    union 
    
    select
        '00000000'      AS gerente_id,
        'Não Definido'  AS nome,
        'Não Definido'  AS nome_fantasia,
        'Não Definido'  AS cidade,
        'ND'            AS estado

)

select * from final