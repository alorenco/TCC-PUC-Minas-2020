
with flan as (

    select * from staging.flan

),

flanbaixa as (

    select 
        codcoligada,
        idlan,
        sum(valor) valor_baixa

    from {{ ref('stg_lancamentos_baixas') }}

    group by
        codcoligada,
        idlan

),

final as (

    select
        flan.codcoligada,
        flan.idlan,
        flan.statuslan,
        flan.codaplicacao,
        flan.codtdo,
        flan.datavencimento,

        -- valor a pagar
        (flan.valororiginal + flan.valorjuros + 
         flan.valorop1 + flan.valoracrescimoacordo + 
         flan.valorjurosacordo - flan.valordesconto) AS valor,

        -- valor pago
        coalesce(flanbaixa.valor_baixa, 0) valor_pago,         

        -- valor aberto
        case
            when flan.statuslan in (1,2) then 0
            else (flan.valororiginal + flan.valorjuros +  
                  flan.valorop1 + flan.valoracrescimoacordo + 
                  flan.valorjurosacordo - flan.valordesconto)  - coalesce(flanbaixa.valor_baixa, 0) 
        end AS valor_aberto

    from flan

    left join flanbaixa
        on flan.codcoligada = flanbaixa.codcoligada
       and flan.idlan = flanbaixa.idlan 

    where
        flan.pagrec = 2
)

select * from final