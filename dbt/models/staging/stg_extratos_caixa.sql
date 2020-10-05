
with fxcx as (

    select * from staging.fxcx

),

final as (

    select
        fxcx.codcoligada,
        fxcx.idxcx,
        fxcx.codcolcxa,
        fxcx.codcxa,
        fxcx.tipo,
        fxcx.compensado,
        fxcx.reconciliado,
        fxcx.tipocustodia,
        fxcx.valor,
        fxcx.data,
        fxcx.datacompensacao,
        fxcx.datavencimento

    from fxcx

)

select * from final