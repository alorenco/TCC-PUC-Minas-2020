
with xvenda as (

    select * from staging.xvenda

),

parcela as (

    select * from {{ ref('stg_vendas_parcelas') }}

),

recebimento as (

    select * from {{ ref('stg_lancamentos_baixas') }}

),

recebimento_diversos as (

    select
        xvenda.codcoligada,
        xvenda.num_venda,

        -- valor recebido em ate 10 dias da data da venda, usado para metas e premios de corretores
        sum(
            case
                when recebimento.data_baixa between xvenda.dat_venda and (dat_venda + interval '10 day' )::date and
                     parcela.codtipoparc not in (14,16,21,26,27,70,71)  -- remover permutas e doações
                    then recebimento.valor
                else 0
            end
        ) as recebido_a_vista,

        max(recebimento.data_baixa) as data_ultimo_pagamento 

    from xvenda 

    inner join parcela
        on xvenda.codcoligada = parcela.codcoligada
       and xvenda.num_venda = parcela.numvenda
    
    inner join recebimento
        on parcela.codcoligada = recebimento.codcoligada
       and parcela.idlan = recebimento.idlan

    group by 
        xvenda.codcoligada,
        xvenda.num_venda
),

juros_venda as (

    select
        xvenda.num_venda, 
    
        max(
            case 
                when xregracomponentevenda.cod_compn = 3 
                    then coalesce(xregracomponentevenda.vr_tx_per, 0) 
                else 0 
            end
        ) juros_fixos,
    
        sum(
            case
                when xregracomponentevenda.cod_compn in (3,4) 
                    then 1 
                else 0 
            end
        ) qtd_compn_juroscm    
    
    from
        staging.xvenda
        
    inner join
        staging.xregracomponentevenda on
            xvenda.num_venda = xregracomponentevenda.num_venda
            
    where
        xregracomponentevenda.cod_grupo = (select 
                                               max(x01.codgrupo) 
                                           from 
                                               staging.xvendaparcela x01
                                           inner join
                                               staging.flan x02 on
                                                   x01.codcollan = x02.codcoligada and
                                                   x01.idlan = x02.idlan
                                           where 
                                               xvenda.codcoligada = x01.codcoligada and
                                               xvenda.num_venda = x01.numvenda and
                                               x02.statuslan <> 2 and 
                                               x02.baixapendente not in (8, 9) and
                                               x01.codtipoparc = 2) 
    
    group by
        xvenda.num_venda  
),


final as (

    select
        xvenda.codcoligada,
        xvenda.num_venda,

        -- quantidade de parcelas
        count(1) as quantidade_parcelas,

        -- valor total recebido da venda
        sum(coalesce(parcela.valor_recebido,0)) as valor_recebido,

        -- valor total a receber da venda
        sum(coalesce(parcela.valor_aberto,0)) as valor_a_receber,

        -- valor em atraso
        sum(
            case 
                when parcela.datavencimento < current_date 
                    then coalesce(parcela.valor_aberto,0) 
                else 0 
            end
        ) as valor_em_atraso,

        -- parcelas em atraso
        sum(
            case 
                when parcela.datavencimento < current_date and coalesce(parcela.valor_aberto,0) > 0
                    then 1 
                else 0 
            end
        ) as parcelas_em_atraso,

        -- valor em atraso financeiro
        sum(
            case 
                when (parcela.datavencimento < current_date and parcela.permuta = 'Não') 
                    then coalesce(parcela.valor_aberto,0) 
                else 0 
            end
        ) as valor_em_atraso_financeiro,

        -- valor em atraso permuta
        sum(
            case 
                when (parcela.datavencimento < current_date and parcela.permuta = 'Sim') 
                    then coalesce(parcela.valor_aberto,0) 
                else 0 
            end
        ) as valor_em_atraso_permuta,

        coalesce(recebimento_diversos.recebido_a_vista, 0) as recebido_a_vista,

        -- ultima data de recebimento
        recebimento_diversos.data_ultimo_pagamento,

        -- Sem componentes de Juros e CM, define como Parcelas Fixas Verdadeiro
        case 
            when coalesce(juros_venda.qtd_compn_juroscm, 0) > 0 and coalesce(juros_venda.juros_fixos, 0) > 0 
                then 'Não' 
            else 'Sim' 
        end AS parcelas_fixas,

        -- Juros Fixos negociados no Aditivo
        coalesce(juros_venda.juros_fixos, 0) * 12 AS juros_fixos

    from xvenda

    inner join parcela
        on xvenda.codcoligada = parcela.codcoligada
       and xvenda.num_venda = parcela.numvenda
       and parcela.statuslan <> 2 

    left join recebimento_diversos 
        on xvenda.codcoligada = recebimento_diversos.codcoligada
       and xvenda.num_venda = recebimento_diversos.num_venda

    left join juros_venda 
       on xvenda.num_venda = juros_venda.num_venda

    group by
        xvenda.codcoligada,
        xvenda.num_venda,
        recebimento_diversos.recebido_a_vista,
        recebimento_diversos.data_ultimo_pagamento,
        juros_venda.qtd_compn_juroscm,
        juros_venda.juros_fixos
)

select * from final