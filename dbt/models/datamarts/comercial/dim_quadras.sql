{{
config({
    "post-hook": [
      "{{ index(this, 'quadra_id')}}",
    ],
    })
}}

with quadras as (

    select * from {{ ref('stg_quadras') }}

),

final as (

    select
        quadras.quadra_id,
        quadras.cod_pess_empr    as empreendimento_id,
        quadras.num_unid         as quadra,
        quadras.dsc_local        as descricao,
        quadras.qtd_sub_unid     as qtd_lotes

    from 
        quadras

)

select * from final