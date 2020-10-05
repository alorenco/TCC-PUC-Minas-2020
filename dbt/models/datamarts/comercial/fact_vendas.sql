{{
config({
    "post-hook": [
      "{{ index(this, 'venda_id')}}",
    ],
    })
}}

with vendas as (

    select * from {{ ref('stg_vendas') }}

),

itens_venda as (

    select * from {{ ref('stg_itens_venda') }}

),

empreendimentos as (

    select * from {{ ref('stg_empreendimentos') }}

),

corretores_venda as (

    select * from {{ ref('stg_corretores_venda') }}

),

gerentes_venda as (

    select * from {{ ref('stg_gerentes_venda') }}

),

final as (

    select
        vendas.num_venda           AS venda_id,
        vendas.codcoligada         AS coligada_id,
        vendas.num_cont            AS contrato,
        vendas.cod_pess_empr       AS empreendimento_id,
        vendas.codcfo              AS cliente_id,
        vendas.dsc_sit_venda       AS situacao,
        vendas.dat_venda           AS data_venda,
        vendas.dat_cancela         AS data_distrato,
        vendas.dat_quit            AS data_quitacao,
        vendas.dsc_tipo_venda      AS tipo_venda,
        vendas.situacao_financeira AS situacao_financeira,
        vendas.modalidade_id       AS modalidade_id,
        vendas.motivo_distrato_id  AS motivo_distrato_id,

        itens_venda.quadra_id      AS quadra_id,
        itens_venda.lote_id        AS lote_id,

        coalesce(corretores_venda.codcfo, '00000000') AS corretor_id,
        coalesce(gerentes_venda.codcfo, '00000000') AS gerente_id,
        coalesce(empreendimentos.localizacao_id, '00|00000') AS local_venda_id,

        -- Valores
        (itens_venda.vr_item - itens_venda.vr_desc + itens_venda.vr_acrescimo)  AS valor_venda,
        vendas.valor_recebido                AS valor_recebido,
        vendas.valor_a_receber               AS valor_a_receber,
        vendas.valor_em_atraso               AS valor_em_atraso,
        vendas.recebido_a_vista              AS recebido_a_vista,
        vendas.parcelas_em_atraso            AS parcelas_em_atraso,
        vendas.valor_em_atraso_financeiro    AS valor_em_atraso_financeiro,
        vendas.valor_em_atraso_permuta       AS valor_em_atraso_permuta,
        vendas.parcelas_fixas                AS parcelas_fixas,
        vendas.juros_fixos                   AS juros_fixos,

        -- Datas
        vendas.data_ultimo_pagamento, 

        -- Flags
        vendas.venda_a_vista          AS venda_a_vista

    from vendas

    inner join itens_venda using (num_venda)

    inner join empreendimentos using (cod_pess_empr)

    left join corretores_venda on
        vendas.codcoligada = corretores_venda.codcoligada and
        vendas.num_venda = corretores_venda.num_venda

    left join gerentes_venda on
        vendas.codcoligada = gerentes_venda.codcoligada and
        vendas.num_venda = gerentes_venda.num_venda

    where 
        vendas.cod_sit_venda <> 10        /* Venda em Preparação */

)

select * from final