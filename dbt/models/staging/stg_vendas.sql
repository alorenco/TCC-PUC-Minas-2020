
with xvenda as (

    select * from staging.xvenda

),

xsituacaovenda as (

    select * from staging.xsituacaovenda

),


distrato as (

    select * from {{ ref('stg_distratos') }}

),

venda_valor as (

    select * from {{ ref('stg_vendas_valores') }}

),


final as (

    select
        xvenda.num_venda,
        xvenda.codcoligada,
        xvenda.num_cont,
        xvenda.cod_pess_empr,
        xvenda.dat_venda,
        xvenda.dat_cancela,
        xvenda.dat_quit,
        xvenda.codcfo,
        xvenda.cod_sit_venda,
        xvenda.cod_mod_venda,
        xsituacaovenda.dsc_sit_venda,
        coalesce(xvenda.tipven, '1')  as tipven,
        coalesce(xvenda.cod_mod_venda, 0) as modalidade_id,

        -- Tipo da Venda
        case
            when coalesce(xvenda.tipven, '1') In ('1', '7') then 'Venda Normal'
            when coalesce(xvenda.tipven, '1') = '2' then 'Desmembramento'
            when coalesce(xvenda.tipven, '1') = '3' then 'Unificação' 
            when coalesce(xvenda.tipven, '1') in ('4', '5') then 'Troca de Lote'
            when coalesce(xvenda.tipven, '1') = '6' then 'Doação'
            when coalesce(xvenda.tipven, '1') = '8' then 'Parceria (Permuta)' 
            when coalesce(xvenda.tipven, '1') = '9' then 'Outros Fins'
            else 'Outros Fins'
        end as dsc_tipo_venda,

        -- Motivo do Distrato
        case
            when xvenda.cod_sit_venda >= 60 then coalesce(distrato.codmotivodistrato, 0)
            else 0 
        end as motivo_distrato_id,

        -- Valores
        coalesce(venda_valor.valor_recebido, 0)                AS valor_recebido,
        coalesce(venda_valor.valor_a_receber, 0)               AS valor_a_receber,
        coalesce(venda_valor.valor_em_atraso, 0)               AS valor_em_atraso,
        coalesce(venda_valor.parcelas_em_atraso, 0)            AS parcelas_em_atraso,
        coalesce(venda_valor.recebido_a_vista, 0)              AS recebido_a_vista,
        coalesce(venda_valor.valor_em_atraso_financeiro, 0)    AS valor_em_atraso_financeiro,
        coalesce(venda_valor.valor_em_atraso_permuta, 0)       AS valor_em_atraso_permuta,
        venda_valor.parcelas_fixas                             AS parcelas_fixas,
        venda_valor.juros_fixos                                AS juros_fixos,

        -- Datas
        case when venda_valor.data_ultimo_pagamento = '1900-01-01' then null else venda_valor.data_ultimo_pagamento end as data_ultimo_pagamento,

        -- Venda Ativa?
        xvenda.cod_sit_venda = 40 AS venda_ativa,

        -- Venda Quitada?
        xvenda.cod_sit_venda = 50 AS venda_quitada,

        -- Venda Ativa?
        xvenda.cod_sit_venda >= 60 AS venda_distratada,

        -- Venda a Vista ?
        case
            when xvenda.dat_quit is null 
                then 'Não'
            when (date_part('day', xvenda.dat_quit::timestamp - xvenda.dat_venda::timestamp) <= 31) or
                 (venda_valor.quantidade_parcelas <= 1) 
                 then 'Sim'
            else
                'ND'
        end as venda_a_vista,

        -- Situação Financeira da Venda
        case 
            when coalesce(venda_valor.valor_em_atraso, 0) > 0
                then 'Inadimplente'
            else 'Adimplente'
        end AS situacao_financeira

    from xvenda

    left join xsituacaovenda 
        on xvenda.cod_sit_venda = xsituacaovenda.cod_sit_venda

    left join distrato 
        on xvenda.num_venda = distrato.numvenda

    left join venda_valor 
        on xvenda.codcoligada = venda_valor.codcoligada
       and xvenda.num_venda = venda_valor.num_venda

)

select * from final