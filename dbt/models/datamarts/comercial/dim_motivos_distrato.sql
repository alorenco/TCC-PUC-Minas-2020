{{
config({
    "post-hook": [
      "{{ index(this, 'motivo_distrato_id')}}",
    ],
    })
}}

with motivos_distrato as (

    select * from {{ ref('stg_motivos_distrato') }}

),

final as (

    select
        motivos_distrato.codmotivodistrato as motivo_distrato_id,
        motivos_distrato.dscmotivodistrato as descricao

    from 
        motivos_distrato

    union 
    
    select
        0 as motivo_distrato_id,
        'Não Definido'  as descricao
)

select * from final