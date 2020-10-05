{{
config({
    "post-hook": [
      "{{ index(this, 'lote_id')}}",
    ],
    })
}}

with lotes as (

    select * from {{ ref('stg_lotes') }}

),

tabelas_preco as (

    select * from {{ ref('stg_tabelas_preco_lote') }}

),

final as (

    select
        lotes.lote_id,
        lotes.cod_pess_empr                     as empreendimento_id,
        lotes.quadra_id                         as quadra_id,
        lotes.num_unid                          as quadra,
        lotes.num_sub_unid                      as lote,
        lotes.descricao                         as descricao,
        lotes.area                              as area_lote,
        lotes.dsc_sit_sub_unid                  as situacao_lote,
        coalesce(tabelas_preco.precosubunid, 0) as preco_venda

    from 
        lotes

    left join
        tabelas_preco on
            lotes.codcoltabprecosubunid = tabelas_preco.codcoltabpreco and
            lotes.numtabprecosubunid = tabelas_preco.numtabpreco and
            lotes.lote_id =  tabelas_preco.lote_id 
)

select * from final