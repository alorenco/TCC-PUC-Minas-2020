{{
config({
    "post-hook": [
      "{{ index(this, 'corretor_id')}}",
    ],
    })
}}

with corretores as (

    select * from {{ ref('stg_corretores') }}

),

final as (

    select
        corretores.codcfo            AS corretor_id,
        corretores.nome              AS nome,
        corretores.nomefantasia      AS nome_fantasia,
        corretores.bairro            AS bairro,
        corretores.cidade            AS cidade,
        corretores.codetd            AS estado,
        corretores.tipo_corretor     AS tipo_corretor

    from corretores

    union 
    
    select
        '00000000'      AS corretor_id,
        'Não Definido'  AS nome,
        'Não Definido'  AS nome_fantasia,
        'Não Definido'  AS bairro,
        'Não Definido'  AS cidade,
        'ND'            AS estado,
        'Não Definido'  AS tipo_corretor

)

select * from final