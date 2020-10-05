
with flanbaixa as (

    select * from staging.flanbaixa

),

fxcx as (

    select * from staging.fxcx

),

final as (

    select
        flanbaixa.codcoligada,
        flanbaixa.idlan,
        flanbaixa.idbaixa,
        coalesce(fxcx.datacompensacao, flanbaixa.databaixa) AS data_baixa,

        -- valor da baixa 
        case
            when fxcx.compensado = 1 and fxcx.tipocustodia = 0 and fxcx.codcxa not in ('0013','0014')
                then flanbaixa.valorbaixa - flanbaixa.valormulta - flanbaixa.valorjuros + flanbaixa.valordesconto
            else 0
        end +
 
        case
            when fxcx.compensado = 0 and fxcx.tipocustodia = 0 and fxcx.codcxa not in ('0013','0014') and fxcx.data < '2016-01-01'::date
                then flanbaixa.valorbaixa - flanbaixa.valormulta - flanbaixa.valorjuros + flanbaixa.valordesconto
            else 0
        end +

        case
            when fxcx.tipocustodia = 0 and fxcx.codcxa in ('0013','0014') 
                then flanbaixa.valorbaixa - flanbaixa.valormulta - flanbaixa.valorjuros + flanbaixa.valordesconto
            else 0
        end +

        case
            when fxcx.compensado = 1 and fxcx.tipocustodia <> 0 and fxcx.datavencimento <= current_date
                then flanbaixa.valorbaixa - flanbaixa.valormulta - flanbaixa.valorjuros + flanbaixa.valordesconto
            else 0
        end +
        
        case
            when flanbaixa.valorvinculado > 0
                then flanbaixa.valorvinculado - case when flanbaixa.valorbaixa > 0 then 0 else flanbaixa.valormulta + flanbaixa.valorjuros - flanbaixa.valordesconto end
            else 0
        end +

        case
            when flanbaixa.valornotacreditoadiantamento > 0 or flanbaixa.valordevolucao > 0 or flanbaixa.valornotacredito > 0
                then flanbaixa.valornotacreditoadiantamento + flanbaixa.valordevolucao + flanbaixa.valornotacredito
            else 0
        end +

        case
            when flanbaixa.valorperdafinanceira > 0 or (flanbaixa.valordesconto > 0 and flanbaixa.valorbaixa = 0)
                then flanbaixa.valorperdafinanceira - flanbaixa.valormulta - flanbaixa.valorjuros + flanbaixa.valordesconto
            else 0 
        end AS valor

        -- Falta Baixa por acordo

    from flanbaixa

    left join fxcx
        on flanbaixa.codcolxcx = fxcx.codcoligada
       and flanbaixa.idxcx = fxcx.idxcx 

    where 
        flanbaixa.status <> 1

)

select * from final