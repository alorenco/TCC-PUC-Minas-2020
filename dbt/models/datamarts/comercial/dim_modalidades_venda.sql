{{
config({
    "post-hook": [
      "{{ index(this, 'modalidade_id')}}",
    ],
    })
}}

with modalidades as (

    select * from {{ ref('stg_modalidades_venda') }}

),

final as (

    select
        modalidades.modalidade_id,
        modalidades.dsc_mod_venda as descricao

    from 
        modalidades

    union 
    
    select
        0 as modalidade_id,
        'Não Definida'  AS descricao
)

select * from final