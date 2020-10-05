
with flan as (

    select * from staging.flan

),

flanintegracao as (

    select
        codcoligada,
        idlan,
        sum(coalesce(valor, 0)) as valor
    from 
        staging.flanintegracao
    group by 
        codcoligada,
        idlan

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

permuta as (

    select
        codcoligada,
        idlan,
        1 permuta
    from 
        staging.flanintegracao
    where 
    	codcompn in (18,26,27)	
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

        -- permuta
        case
            when coalesce(permuta.permuta, 0) = 1 then 'Sim'
            else 'Não'
        end as permuta,

        -- valor a receber
        case
            when flan.codtdo not in ('9999', '0102') and codaplicacao = 'X' and COALESCE(flanintegracao.valor, 0) > 0 
                then round(abs(flan.valorop1) + abs(flan.valorop3) + COALESCE(flanintegracao.valor, 0), 2)  
            when flan.codtdo = '0102' and codaplicacao = 'X' and COALESCE(flanintegracao.valor, 0) > 0 
                then round(flan.valororiginal + COALESCE(flanintegracao.valor, 0), 2)  
            when flan.codtdo <> '9999' and codaplicacao = 'X' and COALESCE(flanintegracao.valor, 0) = 0 
                then (flan.valororiginal + abs(flan.valorop1) + flan.valoracrescimoacordo + flan.valorjurosacordo)  
            when flan.codtdo <> '9999' and codaplicacao <> 'X' 
                then (flan.valororiginal + abs(flan.valorop1) + flan.valoracrescimoacordo + flan.valorjurosacordo)  
            when flan.codtdo = '9999'  
                then (flan.valororiginal + flan.valorjuros + abs(flan.valorop1) + flan.valoracrescimoacordo + flan.valorjurosacordo)  
            else 0
        end AS valor,

        -- valor recebido
        coalesce(flanbaixa.valor_baixa, 0) valor_recebido,

        -- valor aberto
        case
            when (flan.statuslan in (1,2) or flan.baixapendente in (8, 9)) then 0
            else case
                    when flan.codtdo not in ('9999', '0102') and codaplicacao = 'X' and COALESCE(flanintegracao.valor, 0) > 0 
                        then round(abs(flan.valorop1) + abs(flan.valorop3) + COALESCE(flanintegracao.valor, 0), 2)  
                    when flan.codtdo = '0102' and codaplicacao = 'X' and COALESCE(flanintegracao.valor, 0) > 0 
                        then round(flan.valororiginal + COALESCE(flanintegracao.valor, 0), 2)  
                    when flan.codtdo <> '9999' and codaplicacao = 'X' and COALESCE(flanintegracao.valor, 0) = 0 
                        then (flan.valororiginal + abs(flan.valorop1) + flan.valoracrescimoacordo + flan.valorjurosacordo)  
                    when flan.codtdo <> '9999' and codaplicacao <> 'X' 
                        then (flan.valororiginal + abs(flan.valorop1) + flan.valoracrescimoacordo + flan.valorjurosacordo)  
                    when flan.codtdo = '9999'  
                        then (flan.valororiginal + flan.valorjuros + abs(flan.valorop1) + flan.valoracrescimoacordo + flan.valorjurosacordo)  
                    else 0
                end - coalesce(flanbaixa.valor_baixa, 0) 
        end AS valor_aberto

    from flan

    left join flanintegracao
        on flan.codcoligada = flanintegracao.codcoligada
       and flan.idlan = flanintegracao.idlan 

    left join flanbaixa
        on flan.codcoligada = flanbaixa.codcoligada
       and flan.idlan = flanbaixa.idlan 

    left join permuta
        on flan.codcoligada = permuta.codcoligada
       and flan.idlan = permuta.idlan 

    where
        flan.pagrec = 1
)

select * from final