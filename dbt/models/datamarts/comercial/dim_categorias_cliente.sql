{{
config({
    "post-hook": [
      "{{ index(this, 'categoria_id')}}",
    ],
    })
}}

with clientes_categorias as (

    select * from {{ ref('stg_categorias_clientes') }}

),

final as (

    select
        categoria_id,
        valor_minimo as faixa_m2_minimo,
        valor_maximo as faixa_m2_maximo,

        case
            when categoria_id = 1 then 'Ouro'
            when categoria_id = 2 then 'Prata'
            when categoria_id = 3 then 'Bronze'
            else 'Não Definido'
        end AS descricao

    from clientes_categorias

    union all

    select
        0                  as categoria_id,
        0                  as valor_m2_minimo,
        0                  as valor_m2_maximo,
        'Não Classificado' as descricao

)

select * from final